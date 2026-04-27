import os
import time
import requests
import numpy as np
import pandas as pd
from py_polymarket_clob_client.client import ClobClient
from py_polymarket_clob_client.models import OrderArgs, OrderType
from py_polymarket_clob_client.constants import POLYGON

# --- הגדרות ליבה (המנצחות) ---
RISK_PER_TRADE = 0.05  # 5% סיכון לטרייד כפי שביקשת
Z_THRESHOLD = 1.2      # סף הסטייה המקורי
MIN_P = 0.08           # סינון זבל
MAX_P = 0.45           # טווח המחיר המנצח
SCAN_INTERVAL = 600    # סריקה כל 10 דקות (דינמיקת זמן אמת)

class PolymarketFullBot:
    def __init__(self):
        # אתחול הלקוח
        self.client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.getenv("POLY_PRIV_KEY"),
            secret=os.getenv("POLY_API_SECRET"),
            passphrase=os.getenv("POLY_API_PASSPHRASE"),
            chain_id=POLYGON
        )
        print(f"🚀 Bot Started | Strategy: Original Z={Z_THRESHOLD} | Risk: {RISK_PER_TRADE*100}%")

    def fetch_live_data(self):
        """Pipeline פנימי למשיכת נתונים ללא צורך בקובץ חיצוני"""
        try:
            resp = requests.get("https://clob.polymarket.com/markets")
            markets = resp.json()
            data = []
            for m in markets:
                if not m.get('closed') and m.get('active'):
                    data.append({
                        'condition_id': m.get('condition_id'),
                        'question': m.get('question'),
                        'trade_price': float(m.get('last_trade_price', 0)),
                        'token_id': next((o['clob_token_id'] for o in m['outcomes'] if o['label'] == 'Yes'), None)
                    })
            return pd.DataFrame(data)
        except Exception as e:
            print(f"⚠️ Fetch Error: {e}")
            return pd.DataFrame()

    def get_dynamic_stake(self):
        """חישוב 5% מהיתרה הנוכחית בארנק"""
        try:
            # במידה ו-get_balance לא זמין, ניתן למשוך ידנית דרך ה-address
            balance_data = self.client.get_balance()
            current_bal = float(balance_data.get('balance', 10000)) # דיפולט 10K לביטחון
            return current_bal * RISK_PER_TRADE
        except:
            return 100.0 # Stake ברירת מחדל אם ה-API לא מחזיר יתרה

    def execute_trade(self, token_id, price, question):
        """ביצוע הפקודה בשוק"""
        stake = self.get_dynamic_stake()
        shares = round(stake / price, 2)
        
        try:
            print(f"💸 EXECUTE: {shares} shares of '{question[:40]}...' at ${price}")
            order = self.client.create_order(OrderArgs(
                price=price,
                amount=shares,
                side="BUY",
                token_id=token_id,
                order_type=OrderType.GTC
            ))
            return order
        except Exception as e:
            print(f"❌ Trade Failed: {e}")
            return None

    def run(self):
        while True:
            try:
                # 1. איסוף נתונים
                df = self.fetch_live_data()
                if df.empty or len(df) < 10:
                    time.sleep(60)
                    continue

                # 2. חישוב ה-Z-Score (לב האסטרטגיה)
                df = df[df['trade_price'] > 0]
                df['zscore'] = (df['trade_price'] - df['trade_price'].mean()) / df['trade_price'].std()

                # 3. איתור סיגנלים לפי תנאי ה-Sniper
                signals = df[
                    (df['trade_price'] >= MIN_P) & 
                    (df['trade_price'] <= MAX_P) & 
                    (df['zscore'].abs() > Z_THRESHOLD)
                ]

                # 4. ביצוע
                for _, row in signals.iterrows():
                    if row['token_id']:
                        self.execute_trade(row['token_id'], row['trade_price'], row['question'])
                        time.sleep(2) # מניעת חסימת Rate Limit

                print(f"💤 Scan finished. Sleeping {SCAN_INTERVAL/60}m...")
                time.sleep(SCAN_INTERVAL)

            except Exception as e:
                print(f"⚠️ Global Error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    bot = PolymarketFullBot()
    bot.run()