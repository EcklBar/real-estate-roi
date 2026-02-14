"""
listings_consumer.py -- Consumes real estate listings from Kafka.
Reads from "new_listings" topic, persists to PostgreSQL stream_listings, and optionally prints.
"""

import hashlib
import json
import os
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

# ============================================
# CONFIG
# ============================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "new_listings"
GROUP_ID = "nadlanist-consumer-group"

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "nadlanist")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD")

# Set to True to also print each listing to console
PRINT_LISTINGS = os.getenv("PRINT_LISTINGS", "true").lower() in ("1", "true", "yes")


# ============================================
# KAFKA CONSUMER
# ============================================

def create_consumer() -> Consumer:
    """Create Kafka consumer."""
    return Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
    })


def get_warehouse_conn():
    """Create a connection to the warehouse (for inserting listings)."""
    import psycopg2
    return psycopg2.connect(
        host=WAREHOUSE_HOST,
        port=WAREHOUSE_PORT,
        dbname=WAREHOUSE_DB,
        user=WAREHOUSE_USER,
        password=WAREHOUSE_PASSWORD,
    )


def _listing_id(data: dict, source: str) -> str:
    """Stable id for deduplication. Prefer listing_id; else hash of key fields (e.g. Yad2 has no id)."""
    explicit = data.get("listing_id") or data.get("id")
    if explicit is not None:
        return str(explicit)
    h = hashlib.sha256(
        json.dumps(
            {
                "source": source,
                "city": data.get("city"),
                "street": data.get("street"),
                "price": data.get("price"),
            },
            sort_keys=True,
        ).encode()
    ).hexdigest()
    return f"gen_{h[:16]}"


def save_listing_to_db(conn, listing: dict) -> bool:
    """Insert or update one listing in stream_listings. Returns True if saved."""
    source = listing.get("source", "unknown")
    data = listing.get("listing", {})
    listing_id = _listing_id(data, source)

    price = data.get("price") or 0
    sqm = data.get("sqm")
    price_per_sqm = int(price / sqm) if sqm and sqm > 0 else None
    event_time = listing.get("timestamp")  # ISO string; PostgreSQL accepts it

    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO stream_listings (
                source, listing_id, city, neighborhood, street,
                rooms, sqm, floor, price, price_per_sqm, property_type, url, event_time
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::timestamptz
            )
            ON CONFLICT (source, listing_id) DO UPDATE SET
                city = EXCLUDED.city,
                neighborhood = EXCLUDED.neighborhood,
                street = EXCLUDED.street,
                rooms = EXCLUDED.rooms,
                sqm = EXCLUDED.sqm,
                floor = EXCLUDED.floor,
                price = EXCLUDED.price,
                price_per_sqm = EXCLUDED.price_per_sqm,
                property_type = EXCLUDED.property_type,
                url = EXCLUDED.url,
                event_time = EXCLUDED.event_time,
                created_at = NOW()
            """, (
                source,
                listing_id,
                data.get("city") or None,
                data.get("neighborhood") or None,
                data.get("street") or None,
                float(data["rooms"]) if data.get("rooms") is not None else None,
                int(data["sqm"]) if data.get("sqm") is not None else None,
                int(data["floor"]) if data.get("floor") is not None else None,
                price,
                price_per_sqm,
                data.get("property_type") or None,
                data.get("url") or None,
                event_time,
            ))
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        print(f"  DB error saving listing {source}/{listing_id}: {e}")
        return False
    finally:
        cur.close()


def process_listing(listing: dict, conn=None) -> bool:
    """Save listing to DB; optionally print. Returns True if saved to DB."""
    source = listing.get("source", "unknown")
    data = listing.get("listing", {})
    city = data.get("city", "?")
    neighborhood = data.get("neighborhood", "?")
    price = data.get("price", 0)
    rooms = data.get("rooms", "?")
    sqm = data.get("sqm") or 0
    price_sqm = int(price / sqm) if sqm and sqm > 0 else 0

    saved = False
    if conn and WAREHOUSE_USER and WAREHOUSE_PASSWORD:
        saved = save_listing_to_db(conn, listing)

    if PRINT_LISTINGS:
        is_deal = price_sqm > 0 and price_sqm < 20_000
        deal_flag = " ** DEAL! **" if is_deal else ""
        print(f"  [{source}] {city}/{neighborhood} | {rooms} rooms | "
              f"{price:,} ILS ({price_sqm:,}/sqm){deal_flag}")

    return saved


# ============================================
# MAIN
# ============================================

def main():
    """Consume listings from Kafka and persist to PostgreSQL."""
    print("=" * 60)
    print("Listings Consumer -- Kafka → PostgreSQL")
    print(f"Topic: {TOPIC}")
    print(f"Group: {GROUP_ID}")
    print(f"Broker: {KAFKA_BOOTSTRAP}")
    db_ok = bool(WAREHOUSE_USER and WAREHOUSE_PASSWORD)
    print(f"Warehouse: {WAREHOUSE_HOST}:{WAREHOUSE_PORT}/{WAREHOUSE_DB} ({'connected' if db_ok else 'no credentials — print only'})")
    print("=" * 60)
    print("Waiting for messages... (Ctrl+C to stop)\n")

    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    conn = None
    if db_ok:
        try:
            conn = get_warehouse_conn()
        except Exception as e:
            print(f"Warning: could not connect to warehouse: {e}. Will only print.\n")
            conn = None

    count = 0
    saved_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"  Error: {msg.error()}")
                continue

            value = json.loads(msg.value().decode("utf-8"))
            count += 1
            if process_listing(value, conn):
                saved_count += 1
            if count <= 10 or count % 50 == 0:
                print(f"  #{count} (saved: {saved_count})")

    except KeyboardInterrupt:
        print(f"\nStopping... Consumed {count} messages, saved {saved_count} to DB.")
    finally:
        if conn:
            conn.close()
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()