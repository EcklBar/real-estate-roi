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

NADLAN_GOV_API = "https://www.nadlan.gov.il/Nadlan.REST/Main/GetAssestAndDeals"

# ============================================
# NADLAN.GOV.IL API
# ============================================

def fetch_transactions(city: str, page: int = 1, page_size: int = 50) -> dict:
    """
    Fetch one page of real estate transactions from nadlan.gov.il.

    Args:
        city: City name in Hebrew (e.g., "תל אביב יפו")
        page: Page number for pagination
        page_size: Number of results per page (max ~50)

    Returns:
        dict with transaction data
    """
    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (nadlanist-project)",
    }

    payload = {
        "ObjectID": "",
        "CurrentLavel": 1,
        "PageNo": page,
        "OrderByRecordCount": page_size,
        "OrderByParam": "DEALDATETIME DESC",
        "ObjectIDRecived": "",
        "FilterValue": city,
    }

    try:
        resp = requests.post(NADLAN_GOV_API, json=payload, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        print(f"  Page {page}: Got {len(data.get('AllResults', []))} transactions")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching page {page}: {e}")
        return {}


def fetch_all_transactions(city: str, max_pages: int = 3, delay: float = 2.0) -> list:
    """
    Fetch multiple pages of transactions for a city.
    Respects rate limiting with delay between requests.
    """
    all_transactions = []

    for page in range(1, max_pages + 1):
        print(f"Fetching {city} - page {page}/{max_pages}...")
        data = fetch_transactions(city, page=page)

        results = data.get("AllResults", [])
        if not results:
            print(f"  No more results at page {page}")
            break

        all_transactions.extend(results)

        if page < max_pages:
            time.sleep(delay)

    print(f"Total transactions fetched for {city}: {len(all_transactions)}")
    return all_transactions

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