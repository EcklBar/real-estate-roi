"""
madlan_producer.py -- Fetch Madlan (madlan.co.il) listings and publish to Kafka.
Uses curl_cffi for browser-like requests; parses listing links from for-sale pages.
"""

import json
import os
import re
import time
from datetime import datetime
from urllib.parse import urljoin

from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "new_listings"
MADLAN_BASE = "https://www.madlan.co.il"
MADLAN_FOR_SALE = "https://www.madlan.co.il/for-sale/ישראל"


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "madlan-producer",
    })


def delivery_callback(err, msg):
    if err:
        print(f"  FAILED: {err}")


def fetch_madlan_page(url: str, page: int = 1) -> str:
    """Fetch a Madlan for-sale page. Pagination: ?page=N if supported."""
    try:
        if page > 1:
            u = f"{url}?page={page}" if "?" not in url else f"{url}&page={page}"
        else:
            u = url
        resp = cffi_requests.get(u, impersonate="chrome", timeout=30)
        resp.raise_for_status()
        return resp.text
    except Exception as e:
        print(f"  Error fetching {u}: {e}")
        return ""


def parse_listing_from_link(link, listing_id: str) -> dict | None:
    """
    Parse one listing from a Madlan listing link.
    Link text often looks like: ₪2,300,000 5 חד׳ קומה 3 200 מ"ר דירה, מורן 4, אבן גבירול, רחובות
    """
    text = (link.get_text() or "").strip()
    href = link.get("href") or ""
    if "/listings/" not in href or not text:
        return None

    # Price: ₪1,234,567 or ₪1234567
    price_m = re.search(r"₪\s*([\d,]+)", text.replace(" ", ""))
    price = int(price_m.group(1).replace(",", "")) if price_m else None
    if not price:
        return None

    # Rooms: 4 or 4.5 חד׳
    rooms_m = re.search(r"(\d+(?:\.\d+)?)\s*חד['\u2019]?", text)
    rooms = float(rooms_m.group(1)) if rooms_m else None

    # Floor: קומה 2 or קומת קרקע
    floor = None
    floor_m = re.search(r"קומה\s*(\d+)", text)
    if floor_m:
        floor = int(floor_m.group(1))
    elif "קומת קרקע" in text or "קרקע" in text:
        floor = 0

    # Sqm: 120 מ"ר
    sqm_m = re.search(r"(\d+)\s*מ[\"']?ר", text)
    sqm = int(sqm_m.group(1)) if sqm_m else None

    # Address: after first comma often "street, neighborhood, city" or "street, city"
    parts = [p.strip() for p in text.split(",")]
    city = ""
    neighborhood = ""
    street = ""
    if len(parts) >= 2:
        # Skip price/rooms part; address usually last 2-3 parts
        addr_parts = [p for p in parts[1:] if p and not re.match(r"^₪", p)]
        if len(addr_parts) >= 1:
            city = addr_parts[-1]
        if len(addr_parts) >= 2:
            street = addr_parts[0]
        if len(addr_parts) >= 3:
            neighborhood = addr_parts[1]

    return {
        "source": "madlan",
        "event_type": "new_listing",
        "timestamp": datetime.now().isoformat(),
        "listing": {
            "city": city,
            "neighborhood": neighborhood,
            "street": street,
            "rooms": rooms,
            "sqm": sqm,
            "floor": floor,
            "price": price,
            "property_type": parts[0].split()[-1] if parts else "",
            "listing_id": listing_id,
            "url": urljoin(MADLAN_BASE, href),
        },
    }


def fetch_madlan_listings(page: int = 1) -> list:
    """Fetch listing cards from Madlan for-sale page and parse them."""
    html = fetch_madlan_page(MADLAN_FOR_SALE, page)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    listings = []
    seen_ids = set()

    for link in soup.find_all("a", href=True):
        href = link.get("href", "")
        if "/listings/" not in href:
            continue
        # Extract listing ID from /listings/ID
        match = re.search(r"/listings/([A-Za-z0-9_-]+)", href)
        listing_id = match.group(1) if match else ""
        if listing_id in seen_ids:
            continue
        seen_ids.add(listing_id)
        parsed = parse_listing_from_link(link, listing_id)
        if parsed:
            listings.append(parsed)

    print(f"  Page {page}: {len(listings)} listings")
    return listings


def main():
    print("=" * 60)
    print("Madlan Producer -- Streaming to Kafka")
    print("=" * 60)

    producer = create_producer()
    total = 0

    for page in range(1, 4):
        print(f"\nFetching page {page}...")
        listings = fetch_madlan_listings(page)

        for listing in listings:
            key = listing["listing"].get("city", "").encode("utf-8")
            value = json.dumps(listing, ensure_ascii=False).encode("utf-8")
            producer.produce(
                topic=TOPIC,
                key=key,
                value=value,
                callback=delivery_callback,
            )
            total += 1
            city = listing["listing"].get("city", "?")
            price = listing["listing"].get("price", 0)
            rooms = listing["listing"].get("rooms", "?")
            print(f"  [{total}] {city} | {rooms} rooms | {price:,} ILS")
            producer.poll(0)

        time.sleep(2)

    producer.flush()
    print(f"\nDone! Published {total} Madlan listings to Kafka.")


if __name__ == "__main__":
    main()
