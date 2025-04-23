import os
import requests
from datetime import datetime, timedelta
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TIINGO_API_KEY = os.getenv("TIINGO_API_KEY")
TIINGO_INTRADAY_ENDPOINT = "https://api.tiingo.com/iex/{}/prices"

TABLE_NAME = "price_history_15min_sp500"

print("Starting 15-min delta update with Tiingo API...")

def format_symbol_for_tiingo(symbol):
    return symbol.replace("-", ".")

def get_tickers():
    response = supabase.table("base_table_sp500").select("symbol").execute()
    return [row["symbol"] for row in response.data]

def get_latest_timestamp(symbol):
    try:
        response = supabase.table(TABLE_NAME) \
            .select("timestamp") \
            .eq("symbol", symbol) \
            .order("timestamp", desc=True) \
            .limit(1) \
            .execute()
        if response.data:
            return datetime.fromisoformat(response.data[0]["timestamp"])
    except Exception as e:
        print(f"Error fetching latest timestamp for {symbol}: {e}")
    return None

def fetch_15min_prices(symbol):
    api_symbol = format_symbol_for_tiingo(symbol)
    start_date = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    end_date = datetime.utcnow().strftime("%Y-%m-%d")
    params = {
        "token": TIINGO_API_KEY,
        "resampleFreq": "15min",
        "startDate": start_date,
        "endDate": end_date
    }
    url = TIINGO_INTRADAY_ENDPOINT.format(api_symbol)
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error fetching 15-min prices for {symbol}: {response.status_code}")
    except Exception as e:
        print(f"Exception fetching 15-min prices for {symbol}: {e}")
    return []

def insert_new_prices(symbol, price_data, latest_ts):
    new_records = []
    for entry in price_data:
        try:
            ts = datetime.fromisoformat(entry["date"].replace("Z", "+00:00"))
            if not latest_ts or ts > latest_ts:
                new_records.append({
                    "symbol": symbol,
                    "timestamp": ts.isoformat(),
                    "open": entry.get("open"),
                    "high": entry.get("high"),
                    "low": entry.get("low"),
                    "close": entry.get("close"),
                    "volume": entry.get("volume")
                })
        except Exception as e:
            print(f"Error parsing record for {symbol}: {e}")

    print(f"{symbol}: {len(new_records)} new records to insert.")

    for batch in [new_records[i:i+500] for i in range(0, len(new_records), 500)]:
        try:
            supabase.table(TABLE_NAME).insert(batch).execute()
            print(f"Inserted {len(batch)} records for {symbol}")
        except Exception as e:
            print(f"Error inserting records for {symbol}: {e}")

def main():
    tickers = get_tickers()
    for i, symbol in enumerate(tickers):
        print(f"[{i+1}/{len(tickers)}] Checking {symbol}...")
        latest_ts = get_latest_timestamp(symbol)
        price_data = fetch_15min_prices(symbol)
        if price_data:
            insert_new_prices(symbol, price_data, latest_ts)
        else:
            print(f"No new data for {symbol}")

if __name__ == "__main__":
    main()
