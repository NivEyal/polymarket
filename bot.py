"""
BTC 5M Live Trading Bot — Polymarket CLOB  [PRODUCTION v5]
===========================================================
Improvements in this version:
  #1  ENTRY_PRICE_MAX lowered to 0.26  (avoid overpaying)
  #2  Entry window extended to 131s    (catch more entries)
  #3  No minimum exit price            (take both profits AND losses)
  #4  MAX_SELL_RETRIES = 5             (stop looping on stuck orders)
  #5  Variable position sizing         (tiered by price: small/mid/large)
  #6  WebSocket price feed             (replaces REST polling — ~0ms latency)

Env vars (Render dashboard):
  PK                  — Ethereum private key                [Secret]
  CLOB_API_KEY        — from ApiCreds                       [Secret]
  CLOB_API_SECRET     — from ApiCreds                       [Secret]
  CLOB_API_PASSPHRASE — from ApiCreds                       [Secret]
  START_BALANCE       — Starting USDC $ (default: 3235)
  STATE_FILE          — JSON state path (default: bot_state.json)
  RUN_HOURS           — Runtime hours   (default: 24)
  ALLOW_REENTRY       — "1" = re-enter same candle (default: "0")
  LOG_FILE            — Extra log path  (default: logs.txt)
  DD_PAUSE_PCT        — Drawdown % to pause trading (default: 10)
"""

import os
import sys
import json
import time
import logging
import threading
import queue
import requests
import websocket          # pip install websocket-client
import pandas as pd
import numpy as np
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path

try:
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import (
        ApiCreds,
        MarketOrderArgs,
        OrderArgs,
        OrderType,
    )
    from py_clob_client.constants import POLYGON
    # SignatureType does not exist as an Enum in v0.34.x —
    # the raw integer 0 means EOA (standard wallet signature)
    try:
        from py_clob_client.order_builder.builder import EOA as SIG_EOA
    except ImportError:
        SIG_EOA = 0   # fallback: hardcoded EOA value
except ImportError as e:
    print(f"IMPORT ERROR: {e}\nRun: pip install py-clob-client")
    sys.exit(1)

try:
    from eth_account import Account
except ImportError:
    print("IMPORT ERROR: eth_account\nRun: pip install eth-account")
    sys.exit(1)

# ============================================================
# LOGGING — console + file (#6 logs.txt)
# ============================================================

LOG_FILE = os.environ.get("LOG_FILE", "logs.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger("btc5m")

# ============================================================
# CONFIG
# ============================================================

START_BALANCE    = float(os.environ.get("START_BALANCE", "3235"))
STATE_FILE       = os.environ.get("STATE_FILE", "bot_state.json")
RUN_HOURS        = float(os.environ.get("RUN_HOURS", "24"))
RUN_MINUTES      = int(RUN_HOURS * 60)
ALLOW_REENTRY    = os.environ.get("ALLOW_REENTRY", "0") == "1"
DD_PAUSE_PCT     = float(os.environ.get("DD_PAUSE_PCT", "10"))   # % drawdown to pause

# ── FIX #1: correct entry price range ────────────────────────────────────────
ENTRY_TIME_MIN   = 25
ENTRY_TIME_MAX   = 131    # FIX #2: extended from 71 → 131s
ENTRY_PRICE_MIN  = 0.14
ENTRY_PRICE_MAX  = 0.26   # FIX #1: lowered from 0.45 → 0.26

# ── Exit ─────────────────────────────────────────────────────────────────────
EXIT_TIME_MIN    = 47
EXIT_TIME_MAX    = 267
MIN_HOLD_SEC     = 3
MAX_SELL_RETRIES = 5      # FIX #4: lowered from 20 → 5 (stop looping)
FORCE_EXIT_SEC   = 265
# FIX #3: SELL_DEFER_MIN_RATIO removed — sell at market regardless of price

FEE_BPS          = 100
CLOB_HOST        = "https://clob.polymarket.com"
GAMMA_HOST       = "https://gamma-api.polymarket.com"
CHAIN_ID         = POLYGON
FILL_MIN_RATIO   = 0.95

OUT_TRADES_CSV   = "btc5m_live_trades.csv"
OUT_TICKS_CSV    = "btc5m_live_ticks.csv"
OUT_SUMMARY_CSV  = "btc5m_live_summary.csv"

# ── FIX #5: Variable position sizing ─────────────────────────────────────────
# Tiered by entry price (cheaper = more shares, riskier = fewer $)
# Each tier: (price_max, shares_fixed, label)
# Scaled proportionally if balance differs from reference $3235
POSITION_TIERS = [
    (0.18, 62, "large"),    # price 0.14–0.18 → 62 shares (~$9–11)
    (0.22, 38, "mid"),      # price 0.18–0.22 → 38 shares (~$7–8)
    (0.26, 24, "small"),    # price 0.22–0.26 → 24 shares (~$5–6)
]
REFERENCE_BALANCE = 3235.0   # shares calibrated at this balance

def get_position_shares(entry_price: float, balance: float) -> float:
    """
    Returns variable share count based on entry price tier.
    Scales proportionally to current balance vs reference balance.
    """
    base_shares = 24.0  # default (most conservative)
    for price_max, shares, _ in POSITION_TIERS:
        if entry_price <= price_max:
            base_shares = float(shares)
            break
    scale = balance / REFERENCE_BALANCE
    return round(base_shares * scale, 2)

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
# POSITION DATACLASS
# ============================================================

@dataclass
class Position:
    side:                  str
    token_id:              str
    shares:                float
    entry_price:           float
    cost_usd:              float
    market_start:          int
    entry_ts:              int
    entry_sec:             int
    tier:                  str = "small"    # FIX #5: track which tier was used
    entry_order_id:        str = ""
    sell_attempts:         int = 0
    pending_sell_order_id: str = ""

    @property
    def is_stale(self) -> bool:
        return self.market_start != market_start_5m(now_ts())

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(d: dict) -> "Position":
        known = set(Position.__dataclass_fields__)
        return Position(**{k: v for k, v in d.items() if k in known})

# ============================================================
# FIX #6: WEBSOCKET PRICE FEED
# ============================================================

class PriceFeed:
    """
    Subscribes to Polymarket CLOB WebSocket for real-time price updates.
    Falls back to REST polling if WS is unavailable or stale.

    Two improvements:
      • URL fallback: tries both known WS endpoints in order
      • threading.Event: clean stop() for graceful shutdown
    """

    # Try primary URL first, fall back to secondary if connection fails
    WS_URLS = [
        "wss://ws-subscriptions-clob.polymarket.com/ws/market",
        "wss://clob.polymarket.com/ws",
    ]

    def __init__(self):
        self._prices: dict[str, dict]    = {}
        self._lock                        = threading.Lock()
        self._ws_thread: threading.Thread | None = None
        self._subscribed_tokens: set[str] = set()
        self._ws: websocket.WebSocketApp | None  = None
        self._connected                   = False
        self._stop_event                  = threading.Event()   # clean shutdown
        self._active_url: str | None      = None

    # ── WS callbacks ──────────────────────────────────────────────────────────

    def _on_open(self, ws):
        self._connected = True
        log.info("WS connected: %s", self._active_url)
        with self._lock:
            if self._subscribed_tokens:
                self._send_subscribe(list(self._subscribed_tokens))

    def _on_message(self, ws, raw: str):
        try:
            events = json.loads(raw)
            if not isinstance(events, list):
                events = [events]
            for ev in events:
                self._handle_event(ev)
        except Exception as e:
            log.debug("WS parse error: %s", e)

    def _handle_event(self, ev: dict):
        asset_id = str(ev.get("asset_id") or ev.get("market") or "")
        if not asset_id:
            return
        bid = clamp_price(ev.get("best_bid") or ev.get("bid"), 0.0)
        ask = clamp_price(ev.get("best_ask") or ev.get("ask"), 1.0)
        if bid > 0 or ask < 1.0:
            with self._lock:
                self._prices[asset_id] = {
                    "best_bid": bid,
                    "best_ask": ask,
                    "mid":      (bid + ask) / 2,
                    "ts":       now_ts(),
                }

    def _on_error(self, ws, error):
        log.warning("WS error: %s", error)
        self._connected = False

    def _on_close(self, ws, code, msg):
        log.warning("WS closed code=%s", code)
        self._connected = False

    def _send_subscribe(self, token_ids: list[str]):
        if self._ws and self._connected:
            msg = json.dumps({"assets_ids": token_ids, "type": "market"})
            try:
                self._ws.send(msg)
                log.info("WS subscribed: %d tokens", len(token_ids))
            except Exception as e:
                log.warning("WS send failed: %s", e)

    def _run_forever(self):
        """
        Reconnect loop with URL fallback.
        Tries each URL in WS_URLS in turn. On repeated failure, waits 10s.
        Exits cleanly when _stop_event is set.
        """
        url_idx = 0
        fail_count = 0

        while not self._stop_event.is_set():
            url = self.WS_URLS[url_idx % len(self.WS_URLS)]
            self._active_url = url
            try:
                self._ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )
                self._ws.run_forever(ping_interval=20, ping_timeout=10)
                fail_count = 0          # reset on clean exit
            except Exception as e:
                log.warning("WS run_forever (%s): %s", url, e)
                fail_count += 1

            if self._stop_event.is_set():
                break

            # Alternate URL on each failure
            url_idx += 1
            wait = 10 if fail_count >= len(self.WS_URLS) else 5
            log.info("WS reconnecting in %ds (next: %s)",
                     wait, self.WS_URLS[url_idx % len(self.WS_URLS)])
            self._stop_event.wait(wait)   # interruptible sleep

        log.info("WS feed stopped cleanly")

    def start(self):
        self._stop_event.clear()
        self._ws_thread = threading.Thread(
            target=self._run_forever, daemon=True, name="ws-feed"
        )
        self._ws_thread.start()
        log.info("WS price feed started (urls: %s)", self.WS_URLS)

    def stop(self):
        """Signal the WS thread to stop and close the socket."""
        self._stop_event.set()
        if self._ws:
            try:
                self._ws.close()
            except Exception:
                pass
        log.info("WS feed stop requested")

    def subscribe(self, *token_ids: str):
        new = [t for t in token_ids if t not in self._subscribed_tokens]
        if not new:
            return
        with self._lock:
            self._subscribed_tokens.update(new)
        self._send_subscribe(new)

    def get(self, token_id: str) -> dict | None:
        with self._lock:
            return self._prices.get(token_id)

    # ── REST fallback ─────────────────────────────────────────────────────────

    def fetch_rest(self, up_token: str, down_token: str) -> dict | None:
        payload = [
            {"token_id": up_token,   "side": "BUY"},
            {"token_id": up_token,   "side": "SELL"},
            {"token_id": down_token, "side": "BUY"},
            {"token_id": down_token, "side": "SELL"},
        ]
        data = safe_post(f"{CLOB_HOST}/prices", payload)
        if not data:
            return None
        result = {
            "up_buy":    clamp_price(data.get(up_token,   {}).get("BUY")),
            "up_sell":   clamp_price(data.get(up_token,   {}).get("SELL")),
            "down_buy":  clamp_price(data.get(down_token, {}).get("BUY")),
            "down_sell": clamp_price(data.get(down_token, {}).get("SELL")),
        }
        with self._lock:
            self._prices[up_token] = {
                "best_bid": result["up_sell"],  "best_ask": result["up_buy"],
                "mid": (result["up_buy"] + result["up_sell"]) / 2,
                "ts": now_ts(),
            }
            self._prices[down_token] = {
                "best_bid": result["down_sell"], "best_ask": result["down_buy"],
                "mid": (result["down_buy"] + result["down_sell"]) / 2,
                "ts": now_ts(),
            }
        return result

    def get_prices(self, up_token: str, down_token: str,
                   max_age_sec: int = 8) -> dict | None:
        """WS when fresh, REST fallback when stale."""
        up   = self.get(up_token)
        down = self.get(down_token)
        ts   = now_ts()

        up_fresh   = up   is not None and (ts - up.get("ts",   0)) <= max_age_sec
        down_fresh = down is not None and (ts - down.get("ts", 0)) <= max_age_sec

        if up_fresh and down_fresh:
            return {
                "up_buy":    up["best_ask"],
                "up_sell":   up["best_bid"],
                "down_buy":  down["best_ask"],
                "down_sell": down["best_bid"],
                "source":    "ws",
            }

        log.debug("WS stale — REST fallback")
        result = self.fetch_rest(up_token, down_token)
        if result:
            result["source"] = "rest"
        return result

# ============================================================
# FILL VERIFIER
# ============================================================

def parse_fill(receipt: dict, expected_amount: float,
               is_buy: bool) -> tuple[bool, float, float, float]:
    """
    Returns (is_filled, filled_shares, avg_price, actual_cost_usdc).
    Never returns True without confirmed numeric fill data.
    """
    if receipt is None:
        return False, 0.0, 0.0, 0.0

    status = str(receipt.get("status", "")).lower()
    if status in ("unmatched", "cancelled", "failed", ""):
        log.warning("Order status=%r → not filled", status)
        return False, 0.0, 0.0, 0.0

    size_matched = 0.0
    for key in ("size_matched", "sizeFilled", "filled_size", "amount_filled"):
        raw = receipt.get(key)
        if raw is not None:
            try: size_matched = float(raw); break
            except (TypeError, ValueError): pass

    avg_price = 0.0
    for key in ("price", "avgPrice", "avg_price", "average_price"):
        raw = receipt.get(key)
        if raw is not None:
            try: avg_price = float(raw); break
            except (TypeError, ValueError): pass

    actual_cost = 0.0
    for key in ("cost", "usdc_spent", "amount_spent", "notional", "collateral"):
        raw = receipt.get(key)
        if raw is not None:
            try: actual_cost = float(raw); break
            except (TypeError, ValueError): pass
    if actual_cost <= 0 and size_matched > 0 and avg_price > 0:
        actual_cost = size_matched * avg_price
    if actual_cost <= 0:
        actual_cost = expected_amount

    if size_matched <= 0 or avg_price <= 0:
        log.warning("Receipt missing fill data status=%r size=%.4f price=%.4f",
                    status, size_matched, avg_price)
        return False, 0.0, 0.0, 0.0

    if is_buy:
        if status == "matched":
            return True, size_matched, avg_price, actual_cost
        log.warning("BUY status=%r (not matched)", status)
        return False, 0.0, 0.0, 0.0
    else:
        ratio = size_matched / expected_amount if expected_amount > 0 else 0.0
        if ratio >= FILL_MIN_RATIO:
            return True, size_matched, avg_price, actual_cost
        log.warning("SELL partial ratio=%.2f filled=%.4f expected=%.4f",
                    ratio, size_matched, expected_amount)
        return False, size_matched, avg_price, actual_cost

# ============================================================
# EXCHANGE RECONCILER
# ============================================================

class Reconciler:

    def __init__(self, client: ClobClient):
        self.client = client

    def get_balance(self) -> float:
        try:
            for method_name in ("get_collateral_balance", "get_balance"):
                method = getattr(self.client, method_name, None)
                if method is None:
                    continue
                raw = method()
                if isinstance(raw, dict):
                    for k in ("balance", "usdc", "collateral", "amount"):
                        v = raw.get(k)
                        if v is not None:
                            try: return float(v)
                            except (TypeError, ValueError): pass
                    continue
                return float(raw)
        except Exception as e:
            log.warning("get_balance: %s", e)
        return np.nan

    def get_position_size(self, token_id: str) -> float:
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

VALID_TICK_SIZES = [0.1, 0.01, 0.001, 0.0001]

def snap_price(price: float, tick: float) -> float:
    snapped = round(round(price / tick) * tick, 10)
    return max(tick, min(1.0 - tick, snapped))


def _detect_proxy_wallet(eoa_address: str):
    """Query Polymarket proxy factory on Polygon for the proxy wallet address.
    Polymarket web-UI accounts use a proxy wallet (sig_type=1).
    Returns proxy address string, or None if not found."""
    override = os.environ.get('POLY_FUNDER', '').strip()
    if override:
        log.info("Using POLY_FUNDER override: %s", override)
        return override

    FACTORY = '0xaB45c5A4B0c941a2F231C04C3f49182e1A254052'
    RPCS = [
        'https://polygon-rpc.com',
        'https://rpc.ankr.com/polygon',
        'https://polygon-mainnet.public.blastapi.io',
        'https://1rpc.io/matic',
    ]
    padded = eoa_address.lower().replace('0x', '').zfill(64)
    SELECTORS = ['6f7c37f3', '193c1f76', 'b9f14c40', 'f3f43703', 'a6d6dd01']
    zero_addr = '0x' + '0' * 40
    for rpc in RPCS:
        for sel in SELECTORS:
            data = f'0x{sel}{padded}'
            payload = {'jsonrpc':'2.0','method':'eth_call',
                       'params':[{'to': FACTORY,'data': data},'latest'],'id':1}
            try:
                r = requests.post(rpc, json=payload, timeout=6,
                                  headers={'Content-Type':'application/json'})
                result = r.json().get('result', '')
                if result and len(result) >= 42:
                    addr = '0x' + result[-40:]
                    if addr.lower() != zero_addr:
                        log.info("Proxy wallet detected: %s (rpc=%s sel=%s)", addr, rpc, sel)
                        return addr
            except Exception:
                pass
    log.info("No proxy wallet found — using sig_type=0 (EOA)")
    return None


class PolymarketExecutor:

    def __init__(self, private_key: str):
        self._address = Account.from_key(private_key).address

        # FORCE PROXY WALLET - Polymarket account
        self._sig_type = 1
        self._funder = "0x96c57a30082ddefee59ecd41d11642c6ecc8dcb0"

        log.info(
            "Auth FORCE: sig_type=%d funder=%s address=%s",
            self._sig_type,
            self._funder,
            self._address,
        )

        self.client = ClobClient(
            host=CLOB_HOST,
            chain_id=CHAIN_ID,
            key=private_key,
            signature_type=self._sig_type,
            funder=self._funder,
        )

        try:
            self._setup_creds()
            log.info("CLOB auth OK address=%s", self._address)
        except Exception as e:
            log.error("CLOB auth failed: %s", e)
            sys.exit(1)

        self.reconciler = Reconciler(self.client)

    def _setup_creds(self):
        api_key = os.environ.get("CLOB_API_KEY", "").strip()
        api_secret = os.environ.get("CLOB_API_SECRET", "").strip()
        api_passphrase = os.environ.get("CLOB_API_PASSPHRASE", "").strip()

        if api_key and api_secret and api_passphrase:
            self.client.set_api_creds(
                ApiCreds(
                    api_key=api_key,
                    api_secret=api_secret,
                    api_passphrase=api_passphrase,
                )
            )
            log.info("Auth: explicit API creds from env vars")
        else:
            log.info("Auth: deriving API creds from PK")
            creds = self.client.create_or_derive_api_creds()
            self.client.set_api_creds(creds)

    def _get_tick(self, token_id: str) -> float:
        try:
            data = safe_get(f"{CLOB_HOST}/tick-size?token_id={token_id}")
            if data:
                return float(data.get("minimum_tick_size", 0.01))
        except Exception:
            pass
        return 0.01

    def market_buy_shares(
        self,
        token_id: str,
        shares: float,
        ask_price: float,
    ) -> tuple[bool, float, float, float, str]:

        tick = self._get_tick(token_id)
        price = snap_price(ask_price, tick)
        amount_usdc = round(shares * price, 4)

        log.info(
            "BUY sig=%d funder=%s token=%s shares=%.2f price=%.4f usdc=%.4f",
            self._sig_type,
            self._funder[:10],
            token_id[:12],
            shares,
            price,
            amount_usdc,
        )

        try:
            args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side="BUY",
            )
            signed = self.client.create_order(args)
            receipt = self.client.post_order(signed, OrderType.FOK)
            log.info("BUY receipt: %s", json.dumps(receipt, default=str))

        except Exception as e:
            err = str(e)

            if "order_version_mismatch" in err:
                log.error(
                    "order_version_mismatch with sig_type=%s funder=%s. "
                    "Try changing self._sig_type from 1 to 2.",
                    self._sig_type,
                    self._funder,
                )
            else:
                log.error("BUY exception: %s", e)

            return False, 0.0, 0.0, 0.0, ""

        order_id = str(receipt.get("orderID") or receipt.get("id") or "")
        is_filled, filled, avg_p, cost = parse_fill(
            receipt,
            amount_usdc,
            is_buy=True,
        )

        return is_filled, filled, avg_p, cost, order_id

    def market_sell(
        self,
        token_id: str,
        shares: float,
    ) -> tuple[bool, float, float, str]:

        tick = self._get_tick(token_id)
        bid_price = 0.5

        try:
            book = self.client.get_order_book(token_id)
            if book and book.bids:
                bid_price = float(book.bids[0].price)
        except Exception:
            pass

        price = snap_price(bid_price, tick)

        log.info(
            "SELL sig=%d funder=%s token=%s shares=%.4f price=%.4f",
            self._sig_type,
            self._funder[:10],
            token_id[:12],
            shares,
            price,
        )

        try:
            args = OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side="SELL",
            )
            signed = self.client.create_order(args)
            receipt = self.client.post_order(signed, OrderType.FOK)
            log.info("SELL receipt: %s", json.dumps(receipt, default=str))

        except Exception as e:
            log.error("SELL exception: %s", e)
            return False, 0.0, 0.0, ""

        order_id = str(receipt.get("orderID") or receipt.get("id") or "")
        is_filled, filled, avg_p, _ = parse_fill(
            receipt,
            shares,
            is_buy=False,
        )

        return is_filled, filled, avg_p, order_id

# ============================================================
# STATE PERSISTENCE
# ============================================================

class StateStore:
    def __init__(self, path: str):
        self.path = Path(path)

    def save(self, balance: float, peak_balance: float,
             position, trades_log: list, traded_markets: list):
        data = {
            "balance":        balance,
            "peak_balance":   peak_balance,
            "position":       position.to_dict() if position else None,
            "trades_log":     trades_log,
            "traded_markets": traded_markets,
            "saved_at":       now_ts(),
        }
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, indent=2, default=str))
        tmp.replace(self.path)

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

def choose_entry(prices: dict) -> str | None:
    ub, db = prices["up_buy"], prices["down_buy"]
    up_ok   = ENTRY_PRICE_MIN <= ub <= ENTRY_PRICE_MAX
    down_ok = ENTRY_PRICE_MIN <= db <= ENTRY_PRICE_MAX
    if not up_ok and not down_ok:
        return None
    if up_ok and down_ok:
        return "up" if ub <= db else "down"
    return "up" if up_ok else "down"

def get_tier_label(price: float) -> str:
    for price_max, _, label in POSITION_TIERS:
        if price <= price_max:
            return label
    return "small"

# ============================================================
# LIVE BOT
# ============================================================

class LiveBot:

    def __init__(self, executor: PolymarketExecutor, store: StateStore):
        self.executor         = executor
        self.store            = store
        self.balance          = START_BALANCE
        self.peak_balance     = START_BALANCE
        self.position: Position | None = None
        self.trades_log: list[dict]    = []
        self.traded_markets: list[int] = []
        self._paused          = False   # drawdown pause flag

        self._load_and_reconcile()

    # ── Boot ──────────────────────────────────────────────────────────────────

    def _load_and_reconcile(self):
        saved = self.store.load()
        if saved:
            self.balance        = float(saved.get("balance",      START_BALANCE))
            self.peak_balance   = float(saved.get("peak_balance", self.balance))
            self.trades_log     = saved.get("trades_log", [])
            self.traded_markets = saved.get("traded_markets", [])
            pos_dict            = saved.get("position")
            if pos_dict:
                self.position = Position.from_dict(pos_dict)
            log.info("State loaded: bal=%.2f pos=%s candles=%d",
                     self.balance,
                     self.position.token_id if self.position else "none",
                     len(self.traded_markets))
        else:
            log.info("Fresh start bal=%.2f", self.balance)

        onchain = self.executor.reconciler.get_balance()
        if np.isfinite(onchain) and onchain > 0:
            if abs(onchain - self.balance) > 1.0:
                log.warning("Balance drift: local=%.2f onchain=%.2f → using onchain",
                            self.balance, onchain)
            self.balance = onchain
            self.peak_balance = max(self.peak_balance, onchain)
            log.info("On-chain balance: $%.2f", onchain)

        if self.position is not None:
            onchain_shares = self.executor.reconciler.get_position_size(
                self.position.token_id)
            log.info("Reconcile: local=%.4f onchain=%.4f",
                     self.position.shares, onchain_shares)
            if onchain_shares < self.position.shares * FILL_MIN_RATIO:
                log.warning("Position already closed on-chain — clearing")
                self.position = None
            else:
                self.position.shares = onchain_shares

        self._persist()

    def _persist(self):
        self.store.save(self.balance, self.peak_balance,
                        self.position, self.trades_log, self.traded_markets)

    def _drawdown(self) -> float:
        self.peak_balance = max(self.peak_balance, self.balance)
        return 1.0 - self.balance / self.peak_balance

    def _check_drawdown_pause(self) -> bool:
        """
        FIX #5 (risk management): pause NEW entries when drawdown exceeds threshold.
        Existing position is still managed normally.
        """
        dd_pct = self._drawdown() * 100
        if dd_pct >= DD_PAUSE_PCT:
            if not self._paused:
                log.warning("DRAWDOWN %.1f%% >= %.1f%% — pausing new entries",
                            dd_pct, DD_PAUSE_PCT)
                self._paused = True
            return True
        if self._paused:
            log.info("Drawdown recovered to %.1f%% — resuming entries", dd_pct)
            self._paused = False
        return False

    # ── Entry ─────────────────────────────────────────────────────────────────

    def enter_position(self, side: str, market: dict,
                       prices: dict, ts: int, sec: int) -> tuple[bool, str]:
        ask = prices["up_buy"] if side == "up" else prices["down_buy"]
        if ask <= 0:
            return False, "bad_entry_price"

        # FIX #5: variable position sizing by price tier
        shares = get_position_shares(ask, self.balance)
        cost_estimate = shares * ask

        if cost_estimate < 1.0:
            return False, "position_too_small"
        if self.balance < cost_estimate * 1.1:   # 10% buffer for slippage
            return False, "balance_too_low"

        tier = get_tier_label(ask)
        token_id = market["up_token"] if side == "up" else market["down_token"]

        is_filled, filled_shares, avg_price, actual_cost, order_id = \
            self.executor.market_buy_shares(token_id, shares, ask)

        if not is_filled:
            return False, f"buy_not_filled order={order_id}"

        self.balance -= actual_cost
        self.peak_balance = max(self.peak_balance, self.balance)

        m_start = market_start_5m(ts)
        self.position = Position(
            side=side, token_id=token_id,
            shares=filled_shares, entry_price=avg_price,
            cost_usd=actual_cost, market_start=m_start,
            entry_ts=ts, entry_sec=sec, tier=tier,
            entry_order_id=order_id,
        )

        if m_start not in self.traded_markets:
            self.traded_markets.append(m_start)

        self._persist()
        log.info("POSITION OPEN %s tier=%s shares=%.2f price=%.4f cost=%.2f",
                 side.upper(), tier, filled_shares, avg_price, actual_cost)
        return True, f"entered tier={tier} shares={filled_shares:.2f} cost=${actual_cost:.2f}"

    # ── Exit ──────────────────────────────────────────────────────────────────

    def _should_attempt_exit(self, pos: Position, sec: int) -> bool:
        held = now_ts() - pos.entry_ts
        if held < MIN_HOLD_SEC:
            return False
        if pos.is_stale:
            return True     # stale → sell every tick
        return EXIT_TIME_MIN <= sec <= EXIT_TIME_MAX

    def try_exit(self, prices: dict, ts: int, sec: int) -> dict | None:
        pos = self.position
        if pos is None:
            return None
        if not self._should_attempt_exit(pos, sec):
            return None

        held       = ts - pos.entry_ts
        is_stale   = pos.is_stale
        force_exit = is_stale or (sec >= FORCE_EXIT_SEC) or \
                     (pos.sell_attempts >= MAX_SELL_RETRIES)

        raw_sell = clamp_price(
            prices["up_sell"] if pos.side == "up" else prices["down_sell"]
        )

        # FIX #3: NO minimum price check — sell at market (take profits AND losses)
        # Previously gated by SELL_DEFER_MIN_RATIO = 0.85 — now removed

        # Only defer if: not forced AND price is literally zero (exchange error)
        if not force_exit and raw_sell <= 0.01:
            pos.sell_attempts += 1
            self._persist()
            log.warning("Sell price near zero (%.4f) — deferring attempt=%d",
                        raw_sell, pos.sell_attempts)
            return {"type": "defer"}

        if is_stale:
            log.info("Stale position — selling at market price=%.4f", raw_sell)

        is_filled, filled_shares, avg_sell, sell_order_id = \
            self.executor.market_sell(pos.token_id, pos.shares)

        if not is_filled:
            pos.sell_attempts += 1
            pos.pending_sell_order_id = sell_order_id
            if force_exit or is_stale:
                log.error("SELL FAILED force/stale — STUCK token=%s attempt=%d",
                          pos.token_id, pos.sell_attempts)
            self._persist()
            return {"type": "defer", "stuck": force_exit or is_stale}

        gross_value = filled_shares * avg_sell
        fee         = gross_value * (FEE_BPS / 10_000)
        net_value   = gross_value - fee
        pnl         = net_value - pos.cost_usd

        self.balance += net_value
        self.peak_balance = max(self.peak_balance, self.balance)

        onchain = self.executor.reconciler.get_balance()
        if np.isfinite(onchain) and onchain > 0:
            self.balance = onchain

        trade = {
            "market_start":   fmt(pos.market_start),
            "entry_time":     fmt(pos.entry_ts),
            "exit_time":      fmt(ts),
            "entry_sec":      pos.entry_sec,
            "exit_sec":       sec,
            "side":           pos.side,
            "tier":           pos.tier,
            "entry_price":    pos.entry_price,
            "exit_price":     avg_sell,
            "shares":         filled_shares,
            "cost":           pos.cost_usd,
            "gross_value":    gross_value,
            "fee":            fee,
            "net_value":      net_value,
            "profit":         pnl,
            "profit_pct":     pnl / pos.cost_usd * 100,
            "balance":        self.balance,
            "drawdown_pct":   self._drawdown() * 100,
            "sell_attempts":  pos.sell_attempts,
            "force_exit":     force_exit,
            "stale_exit":     is_stale,
            "hold_seconds":   held,
            "price_source":   prices.get("source", "unknown"),
            "entry_order_id": pos.entry_order_id,
            "sell_order_id":  sell_order_id,
        }

        self.trades_log.append(trade)
        self.position = None
        self._persist()

        log.info("POSITION CLOSED %s tier=%s PnL=%+.2f bal=$%.2f DD=%.2f%%",
                 trade["side"].upper(), trade["tier"],
                 pnl, self.balance, trade["drawdown_pct"])
        return {"type": "exit", "trade": trade}

    # ── Main tick ──────────────────────────────────────────────────────────────

    def on_tick(self, market: dict, prices: dict, ts: int, sec: int) -> list[dict]:
        events    = []
        current_m = market_start_5m(ts)

        if self.position is not None:
            ev = self.try_exit(prices, ts, sec)
            if ev:
                events.append(ev)

        if self.position is not None:
            return events

        if not (ENTRY_TIME_MIN <= sec <= ENTRY_TIME_MAX):
            return events

        # Drawdown pause check
        if self._check_drawdown_pause():
            return events

        if not ALLOW_REENTRY and current_m in self.traded_markets:
            return events

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
            "risk_variant":  "VARIABLE_TIERED",
            "start_balance": START_BALANCE,
            "final_balance": self.balance,
            "return_pct":    (self.balance / START_BALANCE - 1) * 100,
            "trades":        len(trades),
        }
        if trades.empty:
            return base
        wins   = trades[trades["profit"] > 0]
        losses = trades[trades["profit"] <= 0]
        gp, gl = wins["profit"].sum(), abs(losses["profit"].sum())
        dd     = trades["balance"] / trades["balance"].cummax() - 1
        result = {
            **base,
            "win_rate_pct":     (trades["profit"] > 0).mean() * 100,
            "avg_pnl":          trades["profit"].mean(),
            "median_pnl":       trades["profit"].median(),
            "best_trade":       trades["profit"].max(),
            "worst_trade":      trades["profit"].min(),
            "profit_factor":    gp / gl if gl > 0 else float("inf"),
            "max_drawdown_pct": dd.min() * 100,
            "stale_exits":      int(trades["stale_exit"].sum()) if "stale_exit" in trades else 0,
            "down_pnl":         trades.loc[trades["side"] == "down", "profit"].sum(),
            "up_pnl":           trades.loc[trades["side"] == "up",   "profit"].sum(),
        }
        # Per-tier breakdown
        if "tier" in trades.columns:
            for tier in ("large", "mid", "small"):
                t = trades[trades["tier"] == tier]
                result[f"trades_{tier}"] = len(t)
                result[f"pnl_{tier}"]    = t["profit"].sum() if len(t) else 0.0
        return result

# ============================================================
# RUNNER
# ============================================================

class Runner:

    def __init__(self):
        pk            = self._load_pk()
        self.executor = PolymarketExecutor(pk)
        self.store    = StateStore(STATE_FILE)
        self.bot      = LiveBot(self.executor, self.store)
        self.feed     = PriceFeed()    # FIX #6: WebSocket feed
        self.ticks_log: list[dict] = []

    @staticmethod
    def _load_pk() -> str:
        pk = os.environ.get("PK", "").strip()
        if not pk:
            log.error("PK env var not set — add it in Render → Environment")
            sys.exit(1)
        return pk if pk.startswith("0x") else "0x" + pk

    def save_all(self):
        pd.DataFrame(self.bot.trades_log).to_csv(OUT_TRADES_CSV, index=False)
        pd.DataFrame(self.ticks_log).to_csv(OUT_TICKS_CSV, index=False)
        pd.DataFrame([self.bot.summary()]).to_csv(OUT_SUMMARY_CSV, index=False)

    def print_summary(self):
        s = self.bot.summary()
        log.info("=" * 70)
        log.info("FINAL SUMMARY")
        for k, v in s.items():
            log.info("  %-28s %s", k, f"{v:.4f}" if isinstance(v, float) else v)
        log.info("=" * 70)

    def run(self, minutes: int = RUN_MINUTES):
        log.info("Starting v5 — variable sizing — %.1fh  reentry=%s  dd_pause=%.1f%%",
                 minutes / 60, ALLOW_REENTRY, DD_PAUSE_PCT)

        # FIX #6: Start WS feed before main loop
        self.feed.start()
        time.sleep(1)   # brief wait for WS handshake

        start       = time.time()
        end         = start + minutes * 60
        last_market = None
        last_rest   = 0   # timestamp of last REST call (throttle)

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
                    time.sleep(1)
                    continue

                up_tok, dn_tok = market["up_token"], market["down_token"]

                # Subscribe WS to this market's tokens
                self.feed.subscribe(up_tok, dn_tok)

                # FIX #6: get prices from WS (instant) or REST (fallback)
                prices = self.feed.get_prices(up_tok, dn_tok)
                if not prices:
                    time.sleep(1)
                    continue

                pos = self.bot.position
                pos_tag = (
                    f"[{pos.side.upper()} {pos.shares:.1f}sh {pos.tier} "
                    f"{'STALE' if pos.is_stale else 'cur'}]"
                    if pos else "[no pos]"
                )
                log.info("t=%3ds %s UP %.2f/%.2f DOWN %.2f/%.2f bal=$%.2f src=%s",
                         sec, pos_tag,
                         prices["up_buy"],  prices["up_sell"],
                         prices["down_buy"], prices["down_sell"],
                         self.bot.balance, prices.get("source", "?"))

                tick = {
                    "time": fmt(ts), "market_start": fmt(m_start),
                    "sec": sec, "slug": slug,
                    "up_buy": prices["up_buy"], "up_sell": prices["up_sell"],
                    "down_buy": prices["down_buy"], "down_sell": prices["down_sell"],
                    "balance": self.bot.balance,
                    "has_position": pos is not None,
                    "price_source": prices.get("source", "?"),
                }

                events = self.bot.on_tick(market, prices, ts, sec)

                for ev in events:
                    if ev["type"] == "entry_attempt" and ev["ok"]:
                        log.info("  ▶ ENTRY %s %s", ev["side"].upper(), ev["msg"])
                    elif ev["type"] == "entry_attempt" and not ev["ok"]:
                        log.info("  ✗ ENTRY failed: %s", ev["msg"])
                    elif ev["type"] == "exit":
                        tr = ev["trade"]
                        log.info(
                            "  ◀ EXIT %s tier=%s PnL=%+.2f bal=$%.2f DD=%.2f%%",
                            tr["side"].upper(), tr["tier"],
                            tr["profit"], tr["balance"], tr["drawdown_pct"],
                        )
                    elif ev.get("stuck"):
                        log.error("  ⚠ POSITION STUCK — check Polymarket manually")

                self.ticks_log.append(tick)
                self.save_all()

                # FIX #6: sleep shorter when WS is live (data arrives via push)
                sleep_sec = 0.5 if prices.get("source") == "ws" else 2.0
                time.sleep(sleep_sec)

        except KeyboardInterrupt:
            log.info("Stopped by user.")
        finally:
            self.feed.stop()   # clean WS shutdown
            self.save_all()
            self.print_summary()


if __name__ == "__main__":
    runner = Runner()
    runner.run()
