"""
yad2_producer.py -- Fetch REAL Yad2 listings and publish to Kafka.
Uses curl_cffi to bypass bot protection.
"""

import json
import os
import time
from datetime import datetime
from curl_cffi import requests as cffi_requests
from bs4 import BeautifulSoup
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "new_listings"
YAD2_URL = "https://www.yad2.co.il/realestate/forsale"


def create_producer() -> Producer:
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "yad2-real-producer",
    })


def delivery_callback(err, msg):
    if err:
        print(f"  FAILED: {err}")


def fetch_yad2_listings(page: int = 1) -> list:
    """Fetch real listings from Yad2 using curl_cffi."""
    url = f"{YAD2_URL}?page={page}"
    try:
        resp = cffi_requests.get(url, impersonate="chrome", timeout=30)
        soup = BeautifulSoup(resp.text, "html.parser")
        script = soup.find("script", id="__NEXT_DATA__")
        if not script:
            print(f"  Page {page}: No data found")
            return []

        data = json.loads(script.string)
        feed = data["props"]["pageProps"]["feed"]

        listings = []
        for source in ["private", "agency", "platinum"]:
            items = feed.get(source, [])
            if isinstance(items, list):
                for item in items:
                    listing = parse_listing(item, source)
                    if listing:
                        listings.append(listing)

        print(f"  Page {page}: {len(listings)} listings")
        return listings

    except Exception as e:
        print(f"  Page {page} error: {e}")
        return []


def parse_listing(item: dict, source: str) -> dict:
    """Parse a single Yad2 listing into a clean format."""
    addr = item.get("address", {})
    details = item.get("additionalDetails", {})

    price = item.get("price")
    if not price:
        return None

    return {
        "source": f"yad2_{source}",
        "event_type": "new_listing",
        "timestamp": datetime.now().isoformat(),
        "listing": {
            "city": addr.get("city", {}).get("text", ""),
            "neighborhood": addr.get("neighborhood", {}).get("text", ""),
            "street": addr.get("street", {}).get("text", ""),
            "rooms": details.get("roomsCount"),
            "sqm": details.get("squareMeter"),
            "floor": addr.get("house", {}).get("floor"),
            "price": price,
            "property_type": details.get("property", {}).get("text", ""),
            "lat": addr.get("coords", {}).get("lat"),
            "lon": addr.get("coords", {}).get("lon"),
        },
    }


def main():
    print("=" * 60)
    print("Yad2 REAL Producer -- Streaming to Kafka")
    print("=" * 60)

    producer = create_producer()
    total = 0

    for page in range(1, 4):  # First 3 pages
        print(f"\nFetching page {page}...")
        listings = fetch_yad2_listings(page)

        for listing in listings:
            key = listing["listing"]["city"]
            value = json.dumps(listing, ensure_ascii=False)
            producer.produce(
                topic=TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_callback,
            )
            total += 1
            city = listing["listing"]["city"]
            price = listing["listing"]["price"]
            rooms = listing["listing"]["rooms"]
            print(f"  [{total}] {city} | {rooms} rooms | {price:,} ILS")
            producer.poll(0)

        time.sleep(3)  # Rate limiting between pages

    producer.flush()
    print(f"\nDone! Published {total} REAL listings to Kafka.")


if __name__ == "__main__":
    main()