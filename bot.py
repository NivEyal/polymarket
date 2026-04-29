"""
BTC 5M Live Trading Bot — Polymarket CLOB  [PRODUCTION]
========================================================
Risk    : RISK_5_PERCENT — fixed 5%
Execution: Real orders via py-clob-client

Critical fixes:
  FIX 1 — Fill verification  : order confirmed by exchange before position opens
  FIX 2 — SELL never ghosts  : position stays until exchange confirms closed
  FIX 3 — Market-change guard: open position NEVER cleared on new candle
  FIX 4 — State persistence  : bot_state.json survives restarts; CLOB reconcile on boot

Required env vars:
  PK            — Ethereum private key (hex, with or without 0x)
  START_BALANCE — Starting USDC balance in $ (default: 100)
  STATE_FILE    — Path to JSON state file     (default: bot_state.json)
  RUN_HOURS     — Total run time in hours     (default: 12)
"""

import os
import sys
import json
import time
import logging
import requests
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import MarketOrderArgs, OrderType, Side
    from py_clob_client.constants import POLYGON
except ImportError:
    print("ERROR: pip install py-clob-client")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("btc5m")

# ============================================================
# CONFIG
# ============================================================

START_BALANCE   = float(os.environ.get("START_BALANCE", "3235"))
STATE_FILE      = os.environ.get("STATE_FILE", "bot_state.json")
RUN_HOURS       = float(os.environ.get("RUN_HOURS", "24"))
RUN_MINUTES     = int(RUN_HOURS * 60)

RISK_PCT        = 0.05   # fixed 5%

ENTRY_TIME_MIN  = 25
ENTRY_TIME_MAX  = 71
ENTRY_PRICE_MIN = 0.14
ENTRY_PRICE_MAX = 0.26

EXIT_TIME_MIN   = 47
EXIT_TIME_MAX   = 267
MIN_HOLD_SEC    = 3
MAX_SELL_RETRIES = 10    # keep retrying every tick until confirmed — never ghost
SELL_DEFER_MIN_RATIO = 0.85
FORCE_EXIT_SEC  = 265

FEE_BPS         = 100
SLIPPAGE_BPS    = 50
POLL_SECONDS    = 2

CLOB_HOST       = "https://clob.polymarket.com"
GAMMA_HOST      = "https://gamma-api.polymarket.com"
CHAIN_ID        = POLYGON

# FOK is all-or-nothing; accept ≥ 95% as dust-rounding
FILL_MIN_RATIO  = 0.95

OUT_TRADES_CSV  = "btc5m_live_trades.csv"
OUT_TICKS_CSV   = "btc5m_live_ticks.csv"
OUT_SUMMARY_CSV = "btc5m_live_summary.csv"

# ============================================================
# HELPERS
# ============================================================

def now_ts() -> int:
    return int(time.time())

def fmt(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def market_start_5m(ts: int) -> int:
    return (ts // 300) * 300

def market_slug(ts: int) -> str:
    return f"btc-updown-5m-{market_start_5m(ts)}"

def clamp_price(x, default=0.5) -> float:
    try:
        x = float(x)
        return max(0.0, min(1.0, x)) if np.isfinite(x) else default
    except Exception:
        return default

def safe_get(url: str, timeout=10):
    try:
        r = requests.get(url, timeout=timeout)
        if r.status_code != 200:
            log.warning("[HTTP %s] %s", r.status_code, url)
            return None
        return r.json()
    except Exception as e:
        log.warning("GET error: %s", e)
        return None

def safe_post(url: str, payload, timeout=10):
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            log.warning("[HTTP %s] %s | %s", r.status_code, url, r.text[:160])
            return None
        return r.json()
    except Exception as e:
        log.warning("POST error: %s", e)
        return None

# ============================================================
# POSITION DATACLASS — fully serialisable to JSON
# ============================================================

@dataclass
class Position:
    side:                  str
    token_id:              str
    shares:                float    # confirmed filled shares (from receipt)
    entry_price:           float    # confirmed avg fill price
    cost_usd:              float    # USDC actually deducted
    market_start:          int
    entry_ts:              int
    entry_sec:             int
    entry_order_id:        str = ""
    sell_attempts:         int = 0
    pending_sell_order_id: str = ""  # last attempted sell order id

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Position":
        return Position(**d)

# ============================================================
# FIX 1 — FILL VERIFIER
# ============================================================

def parse_fill(receipt: dict, expected_amount: float,
               is_buy: bool) -> tuple[bool, float, float]:
    """
    Verify that an order actually filled on the exchange.

    Returns (is_filled, filled_amount, avg_price).
    NEVER returns True unless we have confirmed numeric evidence.

    py-clob-client receipt schema:
      status       : "matched" | "live" | "unmatched" | "cancelled"
      size_matched : shares filled (string)
      price        : average fill price (string)
    """
    if receipt is None:
        return False, 0.0, 0.0

    status = str(receipt.get("status", "")).lower()

    # hard rejection statuses
    if status in ("unmatched", "cancelled", "failed", ""):
        log.warning("Order status=%r → not filled", status)
        return False, 0.0, 0.0

    # extract numeric fields — try multiple key names defensively
    size_matched = 0.0
    for key in ("size_matched", "sizeFilled", "filled_size", "amount_filled"):
        raw = receipt.get(key)
        if raw is not None:
            try:
                size_matched = float(raw)
                break
            except (TypeError, ValueError):
                pass

    avg_price = 0.0
    for key in ("price", "avgPrice", "avg_price", "average_price"):
        raw = receipt.get(key)
        if raw is not None:
            try:
                avg_price = float(raw)
                break
            except (TypeError, ValueError):
                pass

    # both must be positive
    if size_matched <= 0 or avg_price <= 0:
        log.warning("Receipt missing fill data — status=%r size=%.4f price=%.4f",
                    status, size_matched, avg_price)
        return False, 0.0, 0.0

    if is_buy:
        # For BUY we can't pre-compute expected_shares cleanly (price unknown before fill)
        # Just require status==matched + numeric data
        if status == "matched":
            return True, size_matched, avg_price
        log.warning("BUY status=%r (not matched) — not filled", status)
        return False, 0.0, 0.0
    else:
        # For SELL we know expected shares — verify fill ratio
        ratio = size_matched / expected_amount if expected_amount > 0 else 0.0
        if ratio >= FILL_MIN_RATIO:
            return True, size_matched, avg_price
        log.warning("SELL partial fill  ratio=%.2f  filled=%.4f  expected=%.4f",
                    ratio, size_matched, expected_amount)
        return False, size_matched, avg_price

# ============================================================
# EXCHANGE RECONCILER
# ============================================================

class Reconciler:
    """Query exchange for ground truth on restart."""

    def __init__(self, client: ClobClient):
        self.client = client

    def get_balance(self) -> float:
        try:
            return float(self.client.get_balance())
        except Exception as e:
            log.warning("get_balance: %s", e)
            return np.nan

    def get_position_size(self, token_id: str) -> float:
        """Shares of token_id currently held on-chain."""
        try:
            positions = self.client.get_positions() or []
            for p in positions:
                tid = str(p.get("asset_id") or p.get("token_id") or "")
                if tid == token_id:
                    return float(p.get("size") or p.get("amount") or 0.0)
        except Exception as e:
            log.warning("get_position_size token=%s: %s", token_id, e)
        return 0.0

# ============================================================
# POLYMARKET EXECUTOR
# ============================================================

class PolymarketExecutor:

    def __init__(self, private_key: str):
        self.client = ClobClient(
            host=CLOB_HOST, chain_id=CHAIN_ID, key=private_key,
        )
        try:
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)
            log.info("CLOB auth OK  address=%s", self.client.get_address())
        except Exception as e:
            log.error("CLOB auth failed: %s", e)
            sys.exit(1)

        self.reconciler = Reconciler(self.client)

    def market_buy(self, token_id: str, amount_usdc: float
                   ) -> tuple[bool, float, float, str]:
        """
        Place market BUY.
        Returns (is_filled, filled_shares, avg_price, order_id).
        """
        log.info("BUY token=%s usdc=%.2f", token_id, amount_usdc)
        try:
            args    = MarketOrderArgs(token_id=token_id, amount=amount_usdc)
            signed  = self.client.create_market_order(args)
            receipt = self.client.post_order(signed, OrderType.FOK)
            log.info("BUY receipt: %s", json.dumps(receipt, default=str))
        except Exception as e:
            log.error("BUY exception: %s", e)
            return False, 0.0, 0.0, ""

        order_id          = str(receipt.get("orderID") or receipt.get("id") or "")
        is_filled, shares, price = parse_fill(receipt, amount_usdc, is_buy=True)
        return is_filled, shares, price, order_id

    def market_sell(self, token_id: str, shares: float
                    ) -> tuple[bool, float, float, str]:
        """
        Place market SELL.
        Returns (is_filled, filled_shares, avg_price, order_id).
        """
        log.info("SELL token=%s shares=%.4f", token_id, shares)
        try:
            args    = MarketOrderArgs(token_id=token_id, amount=shares, side=Side.SELL)
            signed  = self.client.create_market_order(args)
            receipt = self.client.post_order(signed, OrderType.FOK)
            log.info("SELL receipt: %s", json.dumps(receipt, default=str))
        except Exception as e:
            log.error("SELL exception: %s", e)
            return False, 0.0, 0.0, ""

        order_id                  = str(receipt.get("orderID") or receipt.get("id") or "")
        is_filled, filled, price  = parse_fill(receipt, shares, is_buy=False)
        return is_filled, filled, price, order_id

# ============================================================
# FIX 4 — STATE PERSISTENCE
# ============================================================

class StateStore:
    """
    Atomic JSON file — survives crashes and Render restarts.
    Written after every mutation (entry, exit, deferred sell).
    """

    def __init__(self, path: str):
        self.path = Path(path)

    def save(self, balance: float, peak_balance: float,
             position, trades_log: list):
        data = {
            "balance":      balance,
            "peak_balance": peak_balance,
            "position":     position.to_dict() if position else None,
            "trades_log":   trades_log,
            "saved_at":     now_ts(),
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.replace(self.path)   # atomic

    def load(self) -> dict | None:
        if not self.path.exists():
            return None
        try:
            return json.loads(self.path.read_text())
        except Exception as e:
            log.warning("StateStore.load: %s", e)
            return None

# ============================================================
# MARKET DATA
# ============================================================

def get_market_by_slug(slug: str) -> dict | None:
    data = safe_get(f"{GAMMA_HOST}/markets/slug/{slug}")
    if not data:
        return None
    ids_raw = data.get("clobTokenIds")
    try:
        ids = json.loads(ids_raw) if isinstance(ids_raw, str) else ids_raw
    except Exception:
        return None
    if not ids or len(ids) < 2:
        return None
    return {
        "slug":       slug,
        "question":   data.get("question", slug),
        "up_token":   str(ids[0]),
        "down_token": str(ids[1]),
    }

def get_prices(up_token: str, down_token: str) -> dict | None:
    payload = [
        {"token_id": up_token,   "side": "BUY"},
        {"token_id": up_token,   "side": "SELL"},
        {"token_id": down_token, "side": "BUY"},
        {"token_id": down_token, "side": "SELL"},
    ]
    data = safe_post(f"{CLOB_HOST}/prices", payload)
    if not data:
        return None
    return {
        "up_buy":    clamp_price(data.get(up_token,   {}).get("BUY")),
        "up_sell":   clamp_price(data.get(up_token,   {}).get("SELL")),
        "down_buy":  clamp_price(data.get(down_token, {}).get("BUY")),
        "down_sell": clamp_price(data.get(down_token, {}).get("SELL")),
    }

def choose_entry(prices: dict) -> str | None:
    ub, db = prices["up_buy"], prices["down_buy"]
    up_ok   = ENTRY_PRICE_MIN <= ub <= ENTRY_PRICE_MAX
    down_ok = ENTRY_PRICE_MIN <= db <= ENTRY_PRICE_MAX
    if not up_ok and not down_ok:
        return None
    if up_ok and down_ok:
        return "up" if ub <= db else "down"
    return "up" if up_ok else "down"

# ============================================================
# LIVE BOT
# ============================================================

class LiveBot:

    def __init__(self, executor: PolymarketExecutor, store: StateStore):
        self.executor     = executor
        self.store        = store
        self.balance      = START_BALANCE
        self.peak_balance = START_BALANCE
        self.position: Position | None = None
        self.trades_log: list[dict]    = []

        self._load_and_reconcile()

    # ── FIX 4: boot reconcile ────────────────────────────────────────────────

    def _load_and_reconcile(self):
        saved = self.store.load()

        if saved:
            self.balance      = float(saved.get("balance",      START_BALANCE))
            self.peak_balance = float(saved.get("peak_balance", self.balance))
            self.trades_log   = saved.get("trades_log", [])
            pos_dict          = saved.get("position")
            if pos_dict:
                self.position = Position.from_dict(pos_dict)
            log.info("State loaded: bal=%.2f pos=%s",
                     self.balance, self.position.token_id if self.position else "none")
        else:
            log.info("No saved state — fresh start  bal=%.2f", self.balance)

        # Ground-truth balance from exchange
        onchain = self.executor.reconciler.get_balance()
        if np.isfinite(onchain) and onchain > 0:
            if abs(onchain - self.balance) > 1.0:
                log.warning("Balance drift: local=%.2f onchain=%.2f → using onchain",
                            self.balance, onchain)
            self.balance = onchain
            self.peak_balance = max(self.peak_balance, onchain)
            log.info("On-chain balance: $%.2f", onchain)

        # Verify open position against exchange
        if self.position is not None:
            onchain_shares = self.executor.reconciler.get_position_size(
                self.position.token_id)
            log.info("Reconcile: local=%.4f onchain=%.4f",
                     self.position.shares, onchain_shares)

            if onchain_shares < self.position.shares * FILL_MIN_RATIO:
                log.warning("Position already closed on exchange — clearing local state")
                self.position = None
            else:
                self.position.shares = onchain_shares

        self._persist()

    def _persist(self):
        self.store.save(self.balance, self.peak_balance,
                        self.position, self.trades_log)

    def _drawdown(self) -> float:
        self.peak_balance = max(self.peak_balance, self.balance)
        return 1.0 - self.balance / self.peak_balance

    # ── Entry ─────────────────────────────────────────────────────────────────

    def enter_position(self, side: str, market: dict,
                       prices: dict, ts: int, sec: int) -> tuple[bool, str]:
        bet_usd = self.balance * RISK_PCT
        if bet_usd < 1.0:
            return False, "bet_too_small"
        if self.balance < bet_usd:
            return False, "balance_too_low"

        token_id = market["up_token"] if side == "up" else market["down_token"]
        ask      = prices["up_buy"]   if side == "up" else prices["down_buy"]
        if ask <= 0:
            return False, "bad_entry_price"

        # ── REAL ORDER ────────────────────────────────────────────────────────
        is_filled, shares, avg_price, order_id = \
            self.executor.market_buy(token_id, bet_usd)

        # FIX 1: only open position if exchange confirmed fill
        if not is_filled:
            return False, f"buy_not_filled order={order_id}"

        actual_cost    = shares * avg_price
        self.balance  -= actual_cost
        self.peak_balance = max(self.peak_balance, self.balance)

        self.position = Position(
            side=side, token_id=token_id,
            shares=shares, entry_price=avg_price, cost_usd=actual_cost,
            market_start=market_start_5m(ts), entry_ts=ts, entry_sec=sec,
            entry_order_id=order_id,
        )
        self._persist()   # FIX 4

        log.info("POSITION OPEN %s shares=%.4f price=%.4f cost=%.2f order=%s",
                 side.upper(), shares, avg_price, actual_cost, order_id)
        return True, f"entered 5% bet=${actual_cost:.2f}"

    # ── Exit ──────────────────────────────────────────────────────────────────

    def try_exit(self, prices: dict, ts: int, sec: int) -> dict | None:
        pos = self.position
        if pos is None:
            return None

        held       = ts - pos.entry_ts
        force_exit = (sec >= FORCE_EXIT_SEC) or (pos.sell_attempts >= MAX_SELL_RETRIES)

        if held < MIN_HOLD_SEC:
            return None
        if not (EXIT_TIME_MIN <= sec <= EXIT_TIME_MAX):
            return None

        raw_sell   = clamp_price(prices["up_sell"] if pos.side == "up" else prices["down_sell"])
        min_sell   = pos.entry_price * SELL_DEFER_MIN_RATIO
        would_fill = raw_sell >= min_sell

        if not force_exit and not would_fill:
            pos.sell_attempts += 1
            self._persist()   # FIX 4: save attempt count
            return {"type": "defer"}

        # ── REAL SELL ORDER ───────────────────────────────────────────────────
        is_filled, filled_shares, avg_sell, sell_order_id = \
            self.executor.market_sell(pos.token_id, pos.shares)

        # FIX 2: position stays open until exchange confirms
        if not is_filled:
            pos.sell_attempts += 1
            pos.pending_sell_order_id = sell_order_id

            if force_exit:
                log.error(
                    "SELL FAILED on force_exit — POSITION STUCK  "
                    "token=%s shares=%.4f attempt=%d order=%s — retrying next tick",
                    pos.token_id, pos.shares, pos.sell_attempts, sell_order_id,
                )
            else:
                log.warning("SELL not filled attempt=%d — retrying", pos.sell_attempts)

            self._persist()   # FIX 4: save stuck state
            return {"type": "defer", "stuck": force_exit}

        # ── Confirmed fill ────────────────────────────────────────────────────
        effective_sell = avg_sell * (1 - SLIPPAGE_BPS / 10_000)
        gross_value    = filled_shares * effective_sell
        fee            = gross_value * (FEE_BPS / 10_000)
        net_value      = gross_value - fee
        pnl            = net_value - pos.cost_usd

        self.balance  += net_value
        self.peak_balance = max(self.peak_balance, self.balance)

        # Sync balance from exchange after exit
        onchain = self.executor.reconciler.get_balance()
        if np.isfinite(onchain) and onchain > 0:
            self.balance = onchain

        trade = {
            "market_start":         fmt(pos.market_start),
            "entry_time":           fmt(pos.entry_ts),
            "exit_time":            fmt(ts),
            "entry_sec":            pos.entry_sec,
            "exit_sec":             sec,
            "side":                 pos.side,
            "entry_price":          pos.entry_price,
            "avg_sell_price":       avg_sell,
            "effective_sell_price": effective_sell,
            "shares":               filled_shares,
            "cost":                 pos.cost_usd,
            "risk_pct_used":        RISK_PCT,
            "gross_value":          gross_value,
            "fee":                  fee,
            "net_value":            net_value,
            "profit":               pnl,
            "profit_pct":           pnl / pos.cost_usd * 100,
            "balance":              self.balance,
            "drawdown_pct":         self._drawdown() * 100,
            "sell_attempts":        pos.sell_attempts,
            "force_exit":           force_exit,
            "hold_seconds":         held,
            "entry_order_id":       pos.entry_order_id,
            "sell_order_id":        sell_order_id,
        }

        self.trades_log.append(trade)
        self.position = None      # FIX 2: cleared ONLY after confirmed fill
        self._persist()           # FIX 4

        log.info("POSITION CLOSED %s PnL=%+.2f bal=$%.2f DD=%.2f%%",
                 trade["side"].upper(), pnl, self.balance, trade["drawdown_pct"])
        return {"type": "exit", "trade": trade}

    # ── Main tick ──────────────────────────────────────────────────────────────

    def on_tick(self, market: dict, prices: dict, ts: int, sec: int) -> list[dict]:
        events = []

        # FIX 3: NEVER clear position just because the 5m candle changed
        # Position lifecycle is controlled only by exchange confirmation
        if self.position is not None:
            ev = self.try_exit(prices, ts, sec)
            if ev:
                events.append(ev)

        if self.position is None and ENTRY_TIME_MIN <= sec <= ENTRY_TIME_MAX:
            side = choose_entry(prices)
            if side:
                ok, msg = self.enter_position(side, market, prices, ts, sec)
                events.append({"type": "entry_attempt", "ok": ok,
                                "msg": msg, "side": side, "balance": self.balance})
        return events

    # ── Summary ────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        trades = pd.DataFrame(self.trades_log)
        base = {
            "risk_variant":  "RISK_5_PERCENT",
            "start_balance": START_BALANCE,
            "final_balance": self.balance,
            "return_pct":    (self.balance / START_BALANCE - 1) * 100,
            "trades":        len(trades),
        }
        if trades.empty:
            return base
        wins, losses = trades[trades["profit"] > 0], trades[trades["profit"] <= 0]
        gp, gl = wins["profit"].sum(), abs(losses["profit"].sum())
        dd = (trades["balance"] / trades["balance"].cummax() - 1)
        return {
            **base,
            "win_rate_pct":     (trades["profit"] > 0).mean() * 100,
            "avg_pnl":          trades["profit"].mean(),
            "median_pnl":       trades["profit"].median(),
            "best_trade":       trades["profit"].max(),
            "worst_trade":      trades["profit"].min(),
            "profit_factor":    gp / gl if gl > 0 else float("inf"),
            "max_drawdown_pct": dd.min() * 100,
            "down_pnl":         trades.loc[trades["side"] == "down", "profit"].sum(),
            "up_pnl":           trades.loc[trades["side"] == "up",   "profit"].sum(),
        }

# ============================================================
# RUNNER
# ============================================================

class Runner:

    def __init__(self):
        pk            = self._load_pk()
        self.executor = PolymarketExecutor(pk)
        self.store    = StateStore(STATE_FILE)
        self.bot      = LiveBot(self.executor, self.store)
        self.ticks_log: list[dict] = []

    @staticmethod
    def _load_pk() -> str:
        pk = os.environ.get("PK", "").strip()
        if not pk:
            log.error("PK env var not set")
            sys.exit(1)
        return pk if pk.startswith("0x") else "0x" + pk

    def save_all(self):
        pd.DataFrame(self.bot.trades_log).to_csv(OUT_TRADES_CSV, index=False)
        pd.DataFrame(self.ticks_log).to_csv(OUT_TICKS_CSV, index=False)
        pd.DataFrame([self.bot.summary()]).to_csv(OUT_SUMMARY_CSV, index=False)

    def print_summary(self):
        s = self.bot.summary()
        log.info("=" * 70)
        log.info("FINAL SUMMARY — RISK_5_PERCENT LIVE")
        for k, v in s.items():
            log.info("  %-28s %s", k, f"{v:.4f}" if isinstance(v, float) else v)
        log.info("=" * 70)

    def run(self, minutes: int = RUN_MINUTES):
        log.info("Starting — 5%% risk — %.1fh", minutes / 60)
        start = time.time()
        end   = start + minutes * 60
        last_market = None

        try:
            while time.time() < end:
                ts      = now_ts()
                m_start = market_start_5m(ts)
                sec     = ts - m_start

                if last_market != m_start:
                    last_market = m_start
                    log.info("===== NEW 5M MARKET %s =====", fmt(m_start))

                slug   = market_slug(ts)
                market = get_market_by_slug(slug)
                if not market:
                    time.sleep(POLL_SECONDS)
                    continue

                prices = get_prices(market["up_token"], market["down_token"])
                if not prices:
                    time.sleep(POLL_SECONDS)
                    continue

                pos_tag = (f"[{self.bot.position.side.upper()} "
                           f"{self.bot.position.shares:.2f}sh]"
                           if self.bot.position else "[no pos]")
                log.info("t=%3ds %s UP %.2f/%.2f DOWN %.2f/%.2f bal=$%.2f",
                         sec, pos_tag,
                         prices["up_buy"],  prices["up_sell"],
                         prices["down_buy"], prices["down_sell"],
                         self.bot.balance)

                tick = {
                    "time": fmt(ts), "market_start": fmt(m_start),
                    "sec": sec, "slug": slug,
                    **prices,
                    "balance": self.bot.balance,
                    "has_position": self.bot.position is not None,
                }

                events = self.bot.on_tick(market, prices, ts, sec)

                for ev in events:
                    if ev["type"] == "entry_attempt" and ev["ok"]:
                        log.info("  ▶ ENTRY %s %s", ev["side"].upper(), ev["msg"])
                    elif ev["type"] == "entry_attempt" and not ev["ok"]:
                        log.info("  ✗ ENTRY failed: %s", ev["msg"])
                    elif ev["type"] == "exit":
                        tr = ev["trade"]
                        log.info("  ◀ EXIT %s PnL=%+.2f bal=$%.2f DD=%.2f%%",
                                 tr["side"].upper(), tr["profit"],
                                 tr["balance"], tr["drawdown_pct"])
                    elif ev.get("stuck"):
                        log.error("  ⚠ POSITION STUCK — manual check required")

                self.ticks_log.append(tick)
                self.save_all()
                time.sleep(POLL_SECONDS)

        except KeyboardInterrupt:
            log.info("Stopped by user.")
        finally:
            self.save_all()
            self.print_summary()


if __name__ == "__main__":
    runner = Runner()
    runner.run()
