"""
gov_api.py -- Fetch Israeli real estate data from dirobot.co.il API
and upload raw JSON to MinIO (S3-compatible storage).

Data includes: city summaries, neighborhood stats, price timeseries, 
and individual street-level transactions.
"""

import requests
import json
import os
import io
import time
from datetime import datetime
from urllib.parse import quote
from dotenv import load_dotenv
from minio import Minio

load_dotenv()

# ============================================
# CONFIG
# ============================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
RAW_BUCKET = "nadlanist-raw"

DIROBOT_BASE_URL = "https://api.dirobot.co.il/api/v2"

DIROBOT_HEADERS = {
    "accept": "*/*",
    "origin": "https://www.dirobot.co.il",
    "referer": "https://www.dirobot.co.il/",
    "user-agent": "Mozilla/5.0 (nadlanist-project)",
}

# ============================================
# DIROBOT.CO.IL API
# ============================================

def fetch_cities_summary(min_deals: int = 50) -> dict:
    """Fetch summary of all cities: median prices, deal counts."""
    url = f"{DIROBOT_BASE_URL}/cities-summary?min_deals={min_deals}"
    try:
        resp = requests.get(url, headers=DIROBOT_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        print(f"  Cities summary: {data.get('total_cities', 0)} cities, "
              f"{data.get('total_deals_all_cities', 0)} total deals")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching cities summary: {e}")
        return {}


def get_city_names_from_summary(cities_data: dict, limit: int = 100) -> list:
    """
    Extract list of city names from cities-summary API response.
    Handles different response shapes: list of strings or list of dicts with city/name.
    """
    if not cities_data:
        return []
    raw = cities_data.get("cities") or cities_data.get("items") or cities_data.get("data") or []
    names = []
    for item in raw:
        if isinstance(item, str):
            names.append(item.strip())
        elif isinstance(item, dict):
            name = item.get("city_name") or item.get("city") or item.get("name") or item.get("label")
            if name:
                names.append(str(name).strip())
        if len(names) >= limit:
            break
    return names


def fetch_neighborhoods(city: str, min_deals: int = 5) -> dict:
    """Fetch neighborhood breakdown for a specific city."""
    encoded_city = quote(city, safe="")
    url = f"{DIROBOT_BASE_URL}/neighborhoods-summary?city={encoded_city}&min_deals={min_deals}"
    try:
        resp = requests.get(url, headers=DIROBOT_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        neighborhoods = data.get("neighborhoods", [])
        print(f"  {city}: {len(neighborhoods)} neighborhoods")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching neighborhoods for {city}: {e}")
        return {}


def fetch_city_timeseries(city: str, time_range: str = "all") -> dict:
    """Fetch historical price trends for a city over time."""
    encoded_city = quote(city, safe="")
    url = f"{DIROBOT_BASE_URL}/city-timeseries/{encoded_city}?property_type=apartment&time_range={time_range}"
    try:
        resp = requests.get(url, headers=DIROBOT_HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        rooms = data.get("metadata", {}).get("totalRoomCategories", 0)
        points = data.get("metadata", {}).get("totalDataPoints", 0)
        print(f"  {city} timeseries: {rooms} room categories, {points} data points")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching timeseries for {city}: {e}")
        return {}


def fetch_street_deals(city: str, street: str, per_page: int = 10000) -> dict:
    """Fetch individual transactions for a specific street."""
    city_street = f"{city}_{street}"
    encoded = quote(city_street, safe="")
    url = f"{DIROBOT_BASE_URL}/street-deals/{encoded}?per_page={per_page}"
    try:
        resp = requests.get(url, headers=DIROBOT_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        deals = data.get("deals", [])
        print(f"  {city}, {street}: {len(deals)} deals")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching deals for {city}/{street}: {e}")
        return {}


def fetch_streets_summary(city: str, min_deals: int = 1) -> dict:
    """Fetch list of all streets with deals for a city (API: streets-summary)."""
    encoded_city = quote(city, safe="")
    url = f"{DIROBOT_BASE_URL}/streets-summary?city={encoded_city}&min_deals={min_deals}"
    try:
        resp = requests.get(url, headers=DIROBOT_HEADERS, timeout=60)
        resp.raise_for_status()
        data = resp.json()
        streets = data.get("streets", [])
        print(f"  {city}: {len(streets)} streets from API")
        return data
    except requests.RequestException as e:
        print(f"  Error fetching streets for {city}: {e}")
        return {}


def get_street_list_from_summary(streets_data: dict, limit_per_city: int = None) -> list:
    """
    Extract list of (city, street) from streets-summary API response.
    Returns list of tuples for use with fetch_street_deals.
    """
    if not streets_data:
        return []
    raw = streets_data.get("streets") or []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        city = item.get("city")
        street = item.get("street")
        if city and street:
            out.append((city.strip(), str(street).strip()))
        if limit_per_city and len(out) >= limit_per_city:
            break
    return out

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


def upload_to_minio(data: dict, data_type: str, label: str) -> str:
    """
    Upload data as JSON to MinIO with partitioned path.

    Args:
        data: The data to upload (dict or list)
        data_type: Category folder (e.g., "cities", "neighborhoods", "timeseries", "deals")
        label: Identifier (e.g., city name or "all")

    Path format: dirobot/{data_type}/{label}/year=YYYY/month=MM/data.json
    """
    client = get_minio_client()

    now = datetime.now()
    label_safe = label.replace(" ", "_")
    path = (
        f"dirobot/"
        f"{data_type}/"
        f"{label_safe}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
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

    print(f"  Uploaded: {RAW_BUCKET}/{path} ({len(json_bytes):,} bytes)")
    return path

# ============================================
# MAIN
# ============================================

# Max streets to fetch per city (None = all streets from API; set e.g. 200 to cap runtime)
MAX_STREETS_PER_CITY = None


def main():
    """Fetch real estate data and upload to MinIO. Cities and streets from API (all streets)."""
    print("=" * 60)
    print("Nadlanist -- Dirobot Fetcher (all cities, all streets)")
    print("=" * 60)

    # 1. Cities summary (all Israel) and get dynamic city list
    print("\n[1/4] Fetching cities summary...")
    cities_data = fetch_cities_summary(min_deals=30)
    if cities_data:
        upload_to_minio(cities_data, "cities", "all")

    cities = get_city_names_from_summary(cities_data, limit=80)
    if not cities:
        cities = ["תל אביב יפו", "ירושלים", "חיפה"]
        print(f"  Fallback to default cities: {cities}")
    else:
        print(f"  Using {len(cities)} cities from API for neighborhoods, timeseries, and deals")

    # 2. Neighborhoods for each city
    print("\n[2/4] Fetching neighborhoods...")
    for city in cities:
        neighborhoods_data = fetch_neighborhoods(city)
        if neighborhoods_data:
            upload_to_minio(neighborhoods_data, "neighborhoods", city)
        time.sleep(0.5)

    # 3. Timeseries for each city
    print("\n[3/4] Fetching price timeseries...")
    for city in cities:
        timeseries_data = fetch_city_timeseries(city)
        if timeseries_data:
            upload_to_minio(timeseries_data, "timeseries", city)
        time.sleep(0.5)

    # 4. Street deals: for each city get ALL streets from streets-summary, then fetch deals per street
    print("\n[4/4] Fetching street deals (all streets from API)...")
    total_streets = 0
    for city in cities:
        streets_data = fetch_streets_summary(city, min_deals=1)
        street_list = get_street_list_from_summary(streets_data, limit_per_city=MAX_STREETS_PER_CITY)
        for city_name, street_name in street_list:
            deals_data = fetch_street_deals(city_name, street_name)
            if deals_data and deals_data.get("deals"):
                upload_to_minio(deals_data, "deals", f"{city_name}_{street_name}")
                total_streets += 1
            time.sleep(0.2)
        time.sleep(0.3)
    print(f"  Total street-deal files uploaded: {total_streets}")

    print("\n" + "=" * 60)
    print("Done! Check MinIO Console at http://localhost:9001")
    print("=" * 60)


if __name__ == "__main__":
    main()