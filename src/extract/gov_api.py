"""
gov_api.py -- Fetch real estate transactions from nadlan.gov.il
and upload raw JSON to MinIO (S3-compatible storage).
"""

import requests
import json
import os
import io
import time
from datetime import datetime
from dotenv import load_dotenv
from minio import Minio

# Load .env file (for local development; in Docker, env vars come from docker-compose)
load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
RAW_BUCKET = "nadlanist-raw"

GOV_IL_API = "https://data.gov.il/api/action/datastore_search"
NADLAN_RESOURCE_ID = "5c995652-79a8-4e85-bb67-e648acfea6cc"

# ============================================
# NADLAN.GOV.IL API
# ============================================

def fetch_transactions(city: str, limit: int = 1000, offset: int = 0) -> dict:
    """
    Fetch real estate transactions from data.gov.il CKAN API.

    Args:
        city: City name in Hebrew (e.g., "תל אביב יפו")
        limit: Number of results (max 32000)
        offset: Number of records to skip (for pagination)

    Returns:
        dict with records and total count
    """
    params = {
        "resource_id": NADLAN_RESOURCE_ID,
        "limit": limit,
        "offset": offset,
        "filters": json.dumps({"SETL_NAME": city}),
    }

    try:
        resp = requests.get(GOV_IL_API, params=params, timeout=60)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            print(f"  API error: {data.get('error', 'Unknown')}")
            return {"records": [], "total": 0}

        result = data["result"]
        records = result.get("records", [])
        total = result.get("total", 0)

        print(f"  Got {len(records)} records (total available: {total})")
        return {"records": records, "total": total}

    except requests.RequestException as e:
        print(f"  Error: {e}")
        return {"records": [], "total": 0}
    
def fetch_all_transactions(city: str, batch_size: int = 1000, max_records: int = 3000, delay: float = 1.0) -> list:
    """
    Fetch multiple batches of transactions for a city.
    """
    all_records = []
    offset = 0

    while len(all_records) < max_records:
        print(f"Fetching {city} - offset {offset}...")
        result = fetch_transactions(city, limit=batch_size, offset=offset)

        records = result["records"]
        if not records:
            break

        all_records.extend(records)
        offset += batch_size

        if len(records) < batch_size:
            break

        time.sleep(delay)

    print(f"Total records for {city}: {len(all_records)}")
    return all_records
# ============================================
# MINIO UPLOAD
# ============================================

def get_minio_client() -> Minio:
    """Create MinIO client."""
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upload_to_minio(data: list, city: str) -> str:
    """
    Upload transactions as JSON to MinIO with partitioned path:
    nadlanist-raw/nadlan_gov/city=tel_aviv/year=2025/month=02/day=14/data.json
    """
    client = get_minio_client()

    now = datetime.now()
    city_safe = city.replace(" ", "_")
    path = (
        f"nadlan_gov/"
        f"city={city_safe}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"data.json"
    )

    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    json_stream = io.BytesIO(json_bytes)

    client.put_object(
        bucket_name=RAW_BUCKET,
        object_name=path,
        data=json_stream,
        length=len(json_bytes),
        content_type="application/json",
    )

    print(f"Uploaded to MinIO: {RAW_BUCKET}/{path} ({len(json_bytes):,} bytes)")
    return path

# ============================================
# MAIN
# ============================================

def main():
    """Fetch transactions for major cities and upload to MinIO."""
    print("=" * 60)
    print("Nadlanist -- Gov.il Fetcher")
    print("=" * 60)

    cities = [
        "תל אביב יפו",
        "ירושלים",
        "חיפה",
    ]

    for city in cities:
        print(f"\nProcessing: {city}")
        print("-" * 40)

        # Step 1: Fetch from API
        transactions = fetch_all_transactions(city, batch_size=1000, max_records=3000, delay=1.0)

        if not transactions:
            print(f"No data for {city}, skipping...")
            continue

        # Step 2: Quick data peek
        sample = transactions[0]
        print(f"\nSample transaction:")
        for key, val in list(sample.items())[:8]:
            print(f"   {key}: {val}")

        # Step 3: Upload to MinIO
        upload_to_minio(transactions, city)

    print("\n" + "=" * 60)
    print("Done! Check MinIO Console at http://localhost:9001")
    print("=" * 60)


if __name__ == "__main__":
    main()