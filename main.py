"""
BTC 5M Live Trading Bot — Polymarket CLOB
==========================================
Risk mode : RISK_5_PERCENT (fixed 5%)
Execution : Real orders via py-clob-client
Deploy    : Render (worker service)

Required env vars:
  PK            — Ethereum private key (hex, with or without 0x)
  START_BALANCE — Starting USDC balance in $  (default: 100)
"""

import os
import sys
import time
import json
import logging
import requests
import pandas as pd
import numpy as np
from dataclasses import dataclass, field
from datetime import datetime, timezone
from copy import deepcopy

# ── Polymarket CLOB SDK ────────────────────────────────────────────────────────
try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        MarketOrderArgs,
        OrderType,
        Side,
    )
    from py_clob_client.constants import POLYGON
except ImportError:
    print("ERROR: run  pip install py-clob-client  first")
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

START_BALANCE = float(os.environ.get("START_BALANCE", "100"))

RISK_PCT = 0.05          # RISK_5_PERCENT — fixed

ENTRY_TIME_MIN = 25      # seconds into 5m candle
ENTRY_TIME_MAX = 71
ENTRY_PRICE_MIN = 0.14
ENTRY_PRICE_MAX = 0.26

EXIT_TIME_MIN = 47
EXIT_TIME_MAX = 267
MIN_HOLD_SEC = 3
MAX_SELL_ATTEMPTS = 5
SELL_DEFER_MIN_RATIO = 0.85
FORCE_EXIT_SEC = 265

FEE_BPS = 100
SLIPPAGE_BPS = 50

POLL_SECONDS = 2
RUN_HOURS = float(os.environ.get("RUN_HOURS", "12"))
RUN_MINUTES = int(RUN_HOURS * 60)

CLOB_HOST = "https://clob.polymarket.com"
GAMMA_HOST = "https://gamma-api.polymarket.com"
CHAIN_ID = POLYGON          # 137

OUT_TRADES_CSV = "btc5m_live_trades.csv"
OUT_TICKS_CSV = "btc5m_live_ticks.csv"
OUT_SUMMARY_CSV = "btc5m_live_summary.csv"

# ============================================================
# AUTH — load private key from env
# ============================================================

def load_private_key() -> str:
    pk = os.environ.get("PK", "").strip()
    if not pk:
        log.error("PK env var not set. Export your Ethereum private key as PK=0x...")
        sys.exit(1)
    if not pk.startswith("0x"):
        pk = "0x" + pk
    return pk

# ============================================================
# CLOB CLIENT WRAPPER
# ============================================================

class PolymarketExecutor:
    """
    Thin wrapper around py-clob-client.
    Handles: order placement, position query, balance query.
    """

    def __init__(self, private_key: str):
        self.client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
        )
        # Derive & register API credentials once
        try:
            self.creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(self.creds)
            log.info("CLOB auth OK — address: %s", self.client.get_address())
        except Exception as e:
            log.error("CLOB auth failed: %s", e)
            sys.exit(1)

    def get_balance_usdc(self) -> float:
        """USDC balance on Polygon from CLOB API."""
        try:
            bal = self.client.get_balance()
            return float(bal)
        except Exception as e:
            log.warning("get_balance failed: %s", e)
            return np.nan

    def market_buy(self, token_id: str, amount_usdc: float) -> dict | None:
        """
        Place a market BUY order for `amount_usdc` dollars of `token_id`.
        Returns order receipt dict or None on failure.
        """
        try:
            args = MarketOrderArgs(
                token_id=token_id,
                amount=amount_usdc,          # denominated in USDC (collateral)
            )
            signed = self.client.create_market_order(args)
            resp = self.client.post_order(signed, OrderType.FOK)
            log.info("BUY order: %s", resp)
            return resp
        except Exception as e:
            log.error("market_buy failed token=%s amount=%.2f err=%s", token_id, amount_usdc, e)
            return None

    def market_sell(self, token_id: str, shares: float) -> dict | None:
        """
        Place a market SELL (close) order for `shares` of `token_id`.
        Returns order receipt or None.
        """
        try:
            args = MarketOrderArgs(
                token_id=token_id,
                amount=shares,
                side=Side.SELL,
            )
            signed = self.client.create_market_order(args)
            resp = self.client.post_order(signed, OrderType.FOK)
            log.info("SELL order: %s", resp)
            return resp
        except Exception as e:
            log.error("market_sell failed token=%s shares=%.4f err=%s", token_id, shares, e)
            return None

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
            log.warning("[HTTP %s] %s | %s", r.status_code, url, r.text[:120])
            return None
        return r.json()
    except Exception as e:
        log.warning("POST error: %s", e)
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
        "slug": slug,
        "question": data.get("question", slug),
        "up_token": str(ids[0]),
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
        "up_buy":   clamp_price(data.get(up_token,   {}).get("BUY")),
        "up_sell":  clamp_price(data.get(up_token,   {}).get("SELL")),
        "down_buy": clamp_price(data.get(down_token, {}).get("BUY")),
        "down_sell":clamp_price(data.get(down_token, {}).get("SELL")),
    }

def choose_entry(prices: dict) -> str | None:
    up_buy   = prices["up_buy"]
    down_buy = prices["down_buy"]
    up_ok    = ENTRY_PRICE_MIN <= up_buy   <= ENTRY_PRICE_MAX
    down_ok  = ENTRY_PRICE_MIN <= down_buy <= ENTRY_PRICE_MAX
    if not up_ok and not down_ok:
        return None
    if up_ok and down_ok:
        return "up" if up_buy <= down_buy else "down"
    return "up" if up_ok else "down"

# ============================================================
# POSITION STATE
# ============================================================

@dataclass
class Position:
    side:         str
    token_id:     str
    shares:       float
    entry_price:  float
    cost_usd:     float
    market_start: int
    entry_ts:     int
    entry_sec:    int
    order_id:     str = ""
    sell_attempts: int = 0

# ============================================================
# LIVE BOT — RISK_5_PERCENT
# ============================================================

class LiveBot:
    """
    Executes real orders on Polymarket.
    Risk: fixed 5% of current balance per trade.
    """

    def __init__(self, executor: PolymarketExecutor):
        self.executor = executor
        self.balance = START_BALANCE
        self.peak_balance = START_BALANCE
        self.position: Position | None = None
        self.last_market_start: int | None = None
        self.trades_log: list[dict] = []

    # ── Internal ──────────────────────────────────────────────────────────────

    def _sync_balance(self):
        """Refresh balance from chain. Fallback to local tracking on error."""
        onchain = self.executor.get_balance_usdc()
        if np.isfinite(onchain) and onchain > 0:
            self.balance = onchain
        return self.balance

    def _reset_new_market(self, m_start: int):
        if self.last_market_start != m_start:
            self.position = None
            self.last_market_start = m_start

    def _drawdown(self) -> float:
        self.peak_balance = max(self.peak_balance, self.balance)
        return 1.0 - self.balance / self.peak_balance

    # ── Entry ──────────────────────────────────────────────────────────────────

    def enter_position(self, side: str, market: dict, prices: dict, ts: int, sec: int):
        bet_usd = self.balance * RISK_PCT

        if bet_usd < 1.0:
            return False, "bet_too_small"
        if self.balance < bet_usd:
            return False, "balance_too_low"

        token_id    = market["up_token"] if side == "up" else market["down_token"]
        entry_price = prices["up_buy"]   if side == "up" else prices["down_buy"]

        if entry_price <= 0:
            return False, "bad_entry_price"

        # ── REAL ORDER ────────────────────────────────────────────────────────
        receipt = self.executor.market_buy(token_id, bet_usd)

        if receipt is None:
            return False, "order_rejected"

        # Compute shares from receipt if available, else estimate
        shares_filled = float(receipt.get("size_matched", bet_usd / entry_price))
        avg_price     = float(receipt.get("price", entry_price))
        order_id      = str(receipt.get("orderID", receipt.get("id", "")))

        # Deduct from local balance (chain sync happens next tick)
        self.balance -= bet_usd

        self.position = Position(
            side=side,
            token_id=token_id,
            shares=shares_filled,
            entry_price=avg_price,
            cost_usd=bet_usd,
            market_start=market_start_5m(ts),
            entry_ts=ts,
            entry_sec=sec,
            order_id=order_id,
        )

        log.info("ENTERED %s  bet=$%.2f  price=%.4f  shares=%.4f  order=%s",
                 side.upper(), bet_usd, avg_price, shares_filled, order_id)
        return True, f"entered risk=5% bet=${bet_usd:.2f}"

    # ── Exit ───────────────────────────────────────────────────────────────────

    def try_exit(self, prices: dict, ts: int, sec: int) -> dict | None:
        pos = self.position
        if pos is None:
            return None

        held = ts - pos.entry_ts
        if held < MIN_HOLD_SEC:
            return None
        if not (EXIT_TIME_MIN <= sec <= EXIT_TIME_MAX):
            return None

        raw_sell_price   = clamp_price(prices["up_sell"] if pos.side == "up" else prices["down_sell"])
        min_allowed_sell = pos.entry_price * SELL_DEFER_MIN_RATIO
        force_exit       = (sec >= FORCE_EXIT_SEC) or (pos.sell_attempts >= MAX_SELL_ATTEMPTS)
        would_fill       = raw_sell_price >= min_allowed_sell

        if not force_exit and not would_fill:
            pos.sell_attempts += 1
            return {"type": "defer"}

        # ── REAL SELL ORDER ───────────────────────────────────────────────────
        receipt = self.executor.market_sell(pos.token_id, pos.shares)

        if receipt is None and not force_exit:
            pos.sell_attempts += 1
            return {"type": "defer"}

        # Accept the fill (or estimate on force-exit failure)
        if receipt is not None:
            raw_sell_price = float(receipt.get("price", raw_sell_price))

        effective_sell  = raw_sell_price * (1 - SLIPPAGE_BPS / 10_000)
        gross_value     = pos.shares * effective_sell
        fee             = gross_value * (FEE_BPS / 10_000)
        net_value       = gross_value - fee
        pnl             = net_value - pos.cost_usd

        self.balance   += net_value
        self.peak_balance = max(self.peak_balance, self.balance)

        # Sync from chain after exit
        self._sync_balance()

        trade = {
            "market_start":         fmt(pos.market_start),
            "entry_time":           fmt(pos.entry_ts),
            "exit_time":            fmt(ts),
            "entry_sec":            pos.entry_sec,
            "exit_sec":             sec,
            "side":                 pos.side,
            "entry_price":          pos.entry_price,
            "raw_sell_price":       raw_sell_price,
            "effective_sell_price": effective_sell,
            "shares":               pos.shares,
            "cost":                 pos.cost_usd,
            "risk_pct_used":        RISK_PCT,
            "gross_value":          gross_value,
            "fee":                  fee,
            "net_value":            net_value,
            "profit":               pnl,
            "profit_pct_on_trade":  pnl / pos.cost_usd * 100,
            "balance":              self.balance,
            "drawdown_pct":         self._drawdown() * 100,
            "sell_attempts":        pos.sell_attempts,
            "force_exit":           force_exit,
            "hold_seconds":         held,
            "entry_order_id":       pos.order_id,
        }

        self.trades_log.append(trade)
        self.position = None

        log.info("EXITED %s  PnL=%+.2f  bal=$%.2f  DD=%.2f%%",
                 trade["side"].upper(), pnl, self.balance, trade["drawdown_pct"])
        return {"type": "exit", "trade": trade}

    # ── Main tick ──────────────────────────────────────────────────────────────

    def on_tick(self, market: dict, prices: dict, ts: int, sec: int) -> list[dict]:
        m_start = market_start_5m(ts)
        self._reset_new_market(m_start)
        events = []

        if self.position is not None:
            ev = self.try_exit(prices, ts, sec)
            if ev:
                events.append(ev)

        if self.position is None and ENTRY_TIME_MIN <= sec <= ENTRY_TIME_MAX:
            side = choose_entry(prices)
            if side:
                ok, msg = self.enter_position(side, market, prices, ts, sec)
                events.append({
                    "type": "entry_attempt",
                    "ok": ok,
                    "msg": msg,
                    "side": side,
                    "balance": self.balance,
                })

        return events

    # ── Summary ────────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        trades = pd.DataFrame(self.trades_log)
        base = {
            "risk_variant":  "RISK_5_PERCENT",
            "risk_pct":      RISK_PCT,
            "start_balance": START_BALANCE,
            "final_balance": self.balance,
            "return_pct":    (self.balance / START_BALANCE - 1) * 100,
            "trades":        len(trades),
        }
        if trades.empty:
            return base

        wins   = trades[trades["profit"] > 0]
        losses = trades[trades["profit"] <= 0]
        gross_profit = wins["profit"].sum()
        gross_loss   = abs(losses["profit"].sum())

        eq   = trades["balance"]
        peak = eq.cummax()
        dd   = eq / peak - 1

        return {
            **base,
            "win_rate_pct":      (trades["profit"] > 0).mean() * 100,
            "avg_pnl":           trades["profit"].mean(),
            "median_pnl":        trades["profit"].median(),
            "best_trade":        trades["profit"].max(),
            "worst_trade":       trades["profit"].min(),
            "profit_factor":     gross_profit / gross_loss if gross_loss > 0 else float("inf"),
            "max_drawdown_pct":  dd.min() * 100,
            "down_pnl":          trades.loc[trades["side"] == "down", "profit"].sum(),
            "up_pnl":            trades.loc[trades["side"] == "up",   "profit"].sum(),
        }

# ============================================================
# RUNNER
# ============================================================

class Runner:
    def __init__(self):
        pk            = load_private_key()
        self.executor = PolymarketExecutor(pk)
        self.bot      = LiveBot(self.executor)
        self.ticks_log: list[dict] = []

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
        log.info("Saved: %s | %s | %s", OUT_TRADES_CSV, OUT_TICKS_CSV, OUT_SUMMARY_CSV)

    def run(self, minutes: int = RUN_MINUTES):
        log.info("Starting live bot — 5%% risk — %.1f hours", minutes / 60)

        # Sync balance from chain at startup
        onchain = self.executor.get_balance_usdc()
        if np.isfinite(onchain) and onchain > 0:
            self.bot.balance = onchain
            self.bot.peak_balance = onchain
            log.info("On-chain balance: $%.2f USDC", onchain)
        else:
            log.warning("Could not read on-chain balance, using START_BALANCE=%.2f", START_BALANCE)

        start        = time.time()
        end          = start + minutes * 60
        last_market  = None

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
                    log.debug("t=%ds market not found: %s", sec, slug)
                    time.sleep(POLL_SECONDS)
                    continue

                prices = get_prices(market["up_token"], market["down_token"])
                if not prices:
                    log.debug("t=%ds prices unavailable", sec)
                    time.sleep(POLL_SECONDS)
                    continue

                log.info(
                    "t=%3ds | UP %.2f/%.2f | DOWN %.2f/%.2f | bal=$%.2f",
                    sec,
                    prices["up_buy"],  prices["up_sell"],
                    prices["down_buy"], prices["down_sell"],
                    self.bot.balance,
                )

                tick = {
                    "time":         fmt(ts),
                    "market_start": fmt(m_start),
                    "sec":          sec,
                    "slug":         slug,
                    **prices,
                    "balance":      self.bot.balance,
                    "has_position": self.bot.position is not None,
                }

                events = self.bot.on_tick(market, prices, ts, sec)

                for ev in events:
                    if ev["type"] == "entry_attempt" and ev["ok"]:
                        log.info("  ▶ ENTRY %s %s", ev["side"].upper(), ev["msg"])
                    elif ev["type"] == "exit":
                        tr = ev["trade"]
                        log.info(
                            "  ◀ EXIT %s  PnL=%+.2f  bal=$%.2f  DD=%.2f%%",
                            tr["side"].upper(), tr["profit"], tr["balance"], tr["drawdown_pct"],
                        )

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
