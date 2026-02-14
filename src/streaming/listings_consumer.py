"""
listings_consumer.py -- Consumes real estate listings from Kafka.
Reads from "new_listings" topic, processes, and prints.
"""

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


def process_listing(listing: dict):
    """Process a single listing event."""
    source = listing.get("source", "unknown")
    data = listing.get("listing", {})
    city = data.get("city", "?")
    neighborhood = data.get("neighborhood", "?")
    price = data.get("price", 0)
    rooms = data.get("rooms", "?")
    price_sqm = data.get("price_per_sqm", 0)

    # Simple deal detection: flag listings below 20,000 per sqm
    is_deal = price_sqm > 0 and price_sqm < 20_000
    deal_flag = " ** DEAL! **" if is_deal else ""

    print(f"  [{source}] {city}/{neighborhood} | {rooms} rooms | "
          f"{price:,} ILS ({price_sqm:,}/sqm){deal_flag}")


# ============================================
# MAIN
# ============================================

def main():
    """Consume listings from Kafka."""
    print("=" * 60)
    print("Listings Consumer -- Reading from Kafka")
    print(f"Topic: {TOPIC}")
    print(f"Group: {GROUP_ID}")
    print(f"Broker: {KAFKA_BOOTSTRAP}")
    print("=" * 60)
    print("Waiting for messages... (Ctrl+C to stop)\n")

    consumer = create_consumer()
    consumer.subscribe([TOPIC])

    count = 0

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

            # Parse the message
            value = json.loads(msg.value().decode("utf-8"))
            count += 1

            print(f"\n--- Message #{count} (partition={msg.partition()}, offset={msg.offset()}) ---")
            process_listing(value)

    except KeyboardInterrupt:
        print(f"\nStopping... Consumed {count} messages.")
    finally:
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()