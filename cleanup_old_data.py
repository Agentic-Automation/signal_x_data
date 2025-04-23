import os
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "price_history_15min_sp500"

print("Starting cleanup of records older than 10 days...")

try:
    response = supabase.table(TABLE_NAME) \
        .delete() \
        .lt("timestamp", "now() - interval '10 days'") \
        .execute()

    print(f"Cleanup complete. Deleted {len(response.data)} records.")
except Exception as e:
    print(f"Error during cleanup: {e}")
