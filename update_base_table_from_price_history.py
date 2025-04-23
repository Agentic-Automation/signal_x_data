import os
from datetime import datetime, timedelta, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
import pytz

load_dotenv()

# Supabase config
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_HISTORY = "price_history_15min_sp500"
TABLE_BASE = "base_table_sp500"

print("Starting base table update with 15-min price data...")

# Function to check if market is open

def is_market_open():
    eastern = pytz.timezone("US/Eastern")
    now_utc = datetime.now(timezone.utc)
    now_est = now_utc.astimezone(eastern)
    market_open = now_est.replace(hour=9, minute=30, second=0, microsecond=0)
    market_close = now_est.replace(hour=16, minute=0, second=0, microsecond=0)
    return market_open <= now_est <= market_close

# Fetch tickers
response = supabase.table(TABLE_BASE).select("symbol").execute()
tickers = [row["symbol"] for row in response.data]

for symbol in tickers:
    try:
        # Fetch latest two days of price data
        resp = supabase.table(TABLE_HISTORY) \
            .select("timestamp,current_price") \
            .eq("symbol", symbol) \
            .order("timestamp", desc=True) \
            .limit(96 * 2) \
            .execute()

        entries = sorted(resp.data, key=lambda x: x["timestamp"])
        if not entries:
            print(f"No data for {symbol}, skipping")
            continue

        # Find latest interval and most recent daily closes
        latest_price = entries[-1]["current_price"]
        close_prices = {}
        for e in reversed(entries):
            ts = datetime.fromisoformat(e["timestamp"])
            if ts.hour == 15 and ts.minute == 45:
                date_key = ts.date().isoformat()
                if date_key not in close_prices:
                    close_prices[date_key] = e["current_price"]
                if len(close_prices) >= 2:
                    break

        dates = sorted(close_prices.keys(), reverse=True)
        if is_market_open():
            current_price = latest_price
            last_price = close_prices[dates[0]] if len(dates) >= 1 else None
        else:
            current_price = close_prices[dates[0]] if len(dates) >= 1 else None
            last_price = close_prices[dates[1]] if len(dates) >= 2 else None

        if current_price is None or last_price is None:
            print(f"Insufficient close data for {symbol}, skipping")
            continue

        supabase.table(TABLE_BASE).update({
            "current_price": round(current_price, 2),
            "last_price": round(last_price, 2)
        }).eq("symbol", symbol).execute()
        print(f"Updated {symbol} | Current: {current_price} | Last: {last_price}")

    except Exception as e:
        print(f"Error processing {symbol}: {str(e)}")

print("Base table update completed.")
