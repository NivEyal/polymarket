# BTC 5M Live Bot — Render Deployment Guide

## What this does
- Polls the Polymarket BTC 5-minute UP/DOWN market every 2 seconds
- Enters with **5% of balance** when price is between 0.14–0.26 at t=25–71s
- Exits between t=47–267s (force exit at 265s)
- Places **real CLOB market orders** via `py-clob-client`

---

## Prerequisites

### 1. Polymarket wallet
You need an Ethereum wallet with USDC on **Polygon** (MATIC network).

1. Create a wallet (MetaMask or any EOA)
2. Bridge USDC to Polygon: https://wallet.polygon.technology/
3. Fund your Polymarket account: https://polymarket.com/profile

### 2. Get your private key
Export from MetaMask: Settings → Security → Export Private Key

> **NEVER share this key. Store it only as a Render secret env var.**

---

## Deploy to Render

### Option A — Git deploy (recommended)

1. Push this folder to a GitHub/GitLab repo
2. Go to https://dashboard.render.com → **New → Blueprint**
3. Connect your repo → Render reads `render.yaml` automatically
4. In the service settings → **Environment → Add secret**:
   - `PK` = your Ethereum private key (with or without `0x`)
5. Click **Deploy**

### Option B — Manual service

1. https://dashboard.render.com → **New → Background Worker**
2. Connect repo, set:
   - **Build command**: `pip install -r requirements.txt`
   - **Start command**: `python bot.py`
3. Add env vars:
   - `PK` = `0xYOUR_PRIVATE_KEY` (mark as Secret)
   - `START_BALANCE` = `100` (your starting USDC)
   - `RUN_HOURS` = `12`

---

## Environment variables

| Variable       | Required | Default | Description                          |
|----------------|----------|---------|--------------------------------------|
| `PK`           | ✅ Yes   | —       | Ethereum private key (hex)           |
| `START_BALANCE`| No       | `100`   | Starting USDC balance in $           |
| `RUN_HOURS`    | No       | `12`    | How long to run before stopping      |

---

## Output files

Render workers don't persist disk between deploys. To keep data:
- Connect a **Render Disk** (Starter+ plan) mounted at `/data`
- Or stream logs to an external DB / S3

For now, all data is logged to stdout (visible in Render logs).

| File                        | Contents                     |
|-----------------------------|------------------------------|
| `btc5m_live_trades.csv`     | Every completed trade        |
| `btc5m_live_ticks.csv`      | Every price tick             |
| `btc5m_live_summary.csv`    | Final summary stats          |

---

## Risk parameters (in bot.py)

```python
RISK_PCT = 0.05          # 5% of balance per trade — fixed
ENTRY_PRICE_MIN = 0.14
ENTRY_PRICE_MAX = 0.26
ENTRY_TIME_MIN  = 25     # seconds into 5m candle
ENTRY_TIME_MAX  = 71
FORCE_EXIT_SEC  = 265    # always exit before candle closes
```

---

## Safety checks

- Balance read from **on-chain** at startup
- Orders use **FOK (Fill-or-Kill)** — no partial fills sitting open
- Force-exit at `FORCE_EXIT_SEC=265` regardless of price
- `MAX_SELL_ATTEMPTS=5` before force-exit triggers

---

## Monitoring

View live logs in Render dashboard → your service → **Logs** tab.

Key log lines:
```
t= 35s | UP 0.20/0.18 | DOWN 0.22/0.20 | bal=$100.00
  ▶ ENTRY UP entered risk=5% bet=$5.00
  ◀ EXIT UP  PnL=+1.23  bal=$101.23  DD=0.00%
```
