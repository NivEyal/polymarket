"""
find_proxy.py — מוצא את כתובת ה-proxy wallet של חשבון Polymarket
=================================================================
הרץ:
    python find_proxy.py

ולאחר מכן הוסף לסקריפט ההפעלה:
    set POLY_SIG_TYPE=1
    set POLY_FUNDER=<הכתובת_שנמצאה>
"""

import os, sys, requests

PK = os.environ.get("PK", "").strip()
if not PK:
    print("ERROR: הגדר קודם:  set PK=0xYOUR_PRIVATE_KEY")
    sys.exit(1)

if not PK.startswith("0x"):
    PK = "0x" + PK

from eth_account import Account
EOA = Account.from_key(PK).address
print(f"EOA address: {EOA}\n")

GAMMA = "https://gamma-api.polymarket.com"
CLOB  = "https://clob.polymarket.com"

# ── Method 1: Gamma API profile ──────────────────────────────────────────────
print("Method 1: Gamma API profile...")
try:
    r = requests.get(f"{GAMMA}/profiles", params={"user": EOA}, timeout=10)
    if r.status_code == 200:
        data = r.json()
        profiles = data if isinstance(data, list) else [data]
        for p in profiles:
            proxy = p.get("proxyWallet") or p.get("proxy_wallet") or p.get("funder")
            if proxy:
                print(f"  ✅ FOUND: {proxy}")
                print(f"\n=== הוסף לסקריפט ההפעלה ===")
                print(f"set POLY_SIG_TYPE=1")
                print(f"set POLY_FUNDER={proxy}")
                sys.exit(0)
        print(f"  No proxy in response: {r.text[:300]}")
    else:
        print(f"  HTTP {r.status_code}")
except Exception as e:
    print(f"  Error: {e}")

# ── Method 2: Gamma positions ─────────────────────────────────────────────────
print("\nMethod 2: Gamma positions...")
try:
    r = requests.get(f"{GAMMA}/positions", params={"user": EOA}, timeout=10)
    if r.status_code == 200:
        data = r.json()
        items = data if isinstance(data, list) else data.get("data", [])
        for item in items[:5]:
            for key in ("proxyWallet", "proxy_wallet", "funder", "maker"):
                val = item.get(key)
                if val and val.lower() != EOA.lower():
                    print(f"  ✅ FOUND ({key}): {val}")
                    print(f"\n=== הוסף לסקריפט ההפעלה ===")
                    print(f"set POLY_SIG_TYPE=1")
                    print(f"set POLY_FUNDER={val}")
                    sys.exit(0)
        print(f"  No proxy found in {len(items)} positions")
    else:
        print(f"  HTTP {r.status_code}: {r.text[:200]}")
except Exception as e:
    print(f"  Error: {e}")

# ── Method 3: CLOB trades history ─────────────────────────────────────────────
print("\nMethod 3: CLOB trades (requires API creds)...")
api_key        = os.environ.get("CLOB_API_KEY", "").strip()
api_secret     = os.environ.get("CLOB_API_SECRET", "").strip()
api_passphrase = os.environ.get("CLOB_API_PASSPHRASE", "").strip()

if api_key and api_secret and api_passphrase:
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, TradeParams
        from py_clob_client.order_builder.builder import EOA as SIG_EOA

        client = ClobClient(host=CLOB, chain_id=137, key=PK)
        client.set_api_creds(ApiCreds(
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
        ))

        trades = client.get_trades()
        trade_list = trades if isinstance(trades, list) else trades.get("data", [])

        for t in trade_list[:10]:
            for key in ("maker", "funder", "proxyWallet"):
                val = t.get(key)
                if val and val.lower() != EOA.lower() and len(val) == 42:
                    print(f"  ✅ FOUND ({key}): {val}")
                    print(f"\n=== הוסף לסקריפט ההפעלה ===")
                    print(f"set POLY_SIG_TYPE=1")
                    print(f"set POLY_FUNDER={val}")
                    sys.exit(0)

        if not trade_list:
            print("  No trade history found (new account)")
        else:
            print(f"  Checked {len(trade_list)} trades, no proxy found")
            print(f"  Sample trade keys: {list(trade_list[0].keys())}")

    except Exception as e:
        print(f"  Error: {e}")
else:
    print("  Skipped — CLOB_API_KEY env var not set")

# ── Method 4: Polygon RPC ─────────────────────────────────────────────────────
print("\nMethod 4: Polygon blockchain query...")
FACTORY = "0xaB45c5A4B0c941a2F231C04C3f49182e1A254052"
RPCS = [
    "https://polygon-rpc.com",
    "https://rpc.ankr.com/polygon",
    "https://polygon-mainnet.public.blastapi.io",
    "https://1rpc.io/matic",
]
padded    = EOA.lower().replace("0x", "").zfill(64)
SELECTORS = ["6f7c37f3", "193c1f76", "b9f14c40", "f3f43703", "a6d6dd01"]
zero_addr = "0x" + "0" * 40

found = False
for rpc in RPCS:
    for sel in SELECTORS:
        try:
            r = requests.post(rpc, json={
                "jsonrpc": "2.0", "method": "eth_call",
                "params": [{"to": FACTORY, "data": f"0x{sel}{padded}"}, "latest"],
                "id": 1,
            }, timeout=8, headers={"Content-Type": "application/json"})
            result = r.json().get("result", "")
            if result and len(result) >= 42:
                addr = "0x" + result[-40:]
                if addr.lower() != zero_addr:
                    print(f"  ✅ FOUND: {addr}  (rpc={rpc.split('/')[2]})")
                    print(f"\n=== הוסף לסקריפט ההפעלה ===")
                    print(f"set POLY_SIG_TYPE=1")
                    print(f"set POLY_FUNDER={addr}")
                    found = True
                    sys.exit(0)
        except Exception:
            pass

if not found:
    print("\n" + "="*60)
    print("לא נמצאה כתובת proxy אוטומטית.")
    print()
    print("מצא ידנית:")
    print("1. פתח Chrome → polymarket.com")
    print("2. F12 → Network → לחץ Ctrl+R")
    print("3. חפש בקשות ל-clob.polymarket.com")
    print("4. לחץ על כל בקשה POST ← חפש שדה 'maker' בבקשה")
    print("   OR: חפש 'maker' בכל header/body")
    print()
    print("OR: הרץ בPython:")
    print(f"  import requests")
    print(f"  r = requests.get('https://gamma-api.polymarket.com/profiles?user={EOA}')")
    print(f"  print(r.json())")
