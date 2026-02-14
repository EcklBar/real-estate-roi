"""
yad2_producer.py -- Simulates Yad2 real estate listing events.
Publishes new listings to Kafka topic "new_listings".
"""

import json
import os
import random
import time
from datetime import datetime
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

# ============================================
# CONFIG
# ============================================
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "new_listings"

# Realistic data for simulation
CITIES = {
    "תל אביב יפו": {"min_price": 1_500_000, "max_price": 8_000_000, "neighborhoods": ["פלורנטין", "לב העיר", "נווה צדק", "הצפון הישן", "יפו"]},
    "ירושלים": {"min_price": 1_000_000, "max_price": 6_000_000, "neighborhoods": ["רחביה", "בקעה", "קטמון", "גילה", "מרכז העיר"]},
    "חיפה": {"min_price": 600_000, "max_price": 3_000_000, "neighborhoods": ["כרמל", "דניה", "נווה שאנן", "עיר תחתית", "בת גלים"]},
    "באר שבע": {"min_price": 500_000, "max_price": 2_000_000, "neighborhoods": ["נווה זאב", "רמות", "עיר העתיקה", "נחל עשן"]},
    "רמת גן": {"min_price": 1_200_000, "max_price": 5_000_000, "neighborhoods": ["בורסה", "נחלת גנים", "הגפן", "תל בנימין"]},
}

PROPERTY_TYPES = ["דירה", "דירת גן", "פנטהאוז", "דופלקס", "קוטג'"]


# ============================================
# KAFKA PRODUCER
# ============================================

def create_producer() -> Producer:
    """Create Kafka producer."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "yad2-producer",
    })


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err:
        print(f"  FAILED: {err}")
    else:
        print(f"  Delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def generate_listing() -> dict:
    """Generate a realistic fake Yad2 listing."""
    city = random.choice(list(CITIES.keys()))
    city_data = CITIES[city]

    rooms = random.choice([2, 2.5, 3, 3.5, 4, 4.5, 5])
    sqm = int(rooms * random.uniform(20, 35))
    price = random.randint(city_data["min_price"], city_data["max_price"])

    return {
        "source": "yad2",
        "event_type": "new_listing",
        "timestamp": datetime.now().isoformat(),
        "listing": {
            "city": city,
            "neighborhood": random.choice(city_data["neighborhoods"]),
            "street": f"רחוב {random.randint(1, 200)}",
            "rooms": rooms,
            "sqm": sqm,
            "floor": random.randint(0, 15),
            "total_floors": random.randint(4, 20),
            "price": price,
            "price_per_sqm": round(price / sqm),
            "property_type": random.choice(PROPERTY_TYPES),
            "is_new_building": random.random() < 0.2,
        },
    }


# ============================================
# MAIN
# ============================================

def main():
    """Produce simulated Yad2 listings to Kafka."""
    print("=" * 60)
    print("Yad2 Producer -- Streaming to Kafka")
    print(f"Topic: {TOPIC}")
    print(f"Broker: {KAFKA_BOOTSTRAP}")
    print("=" * 60)

    producer = create_producer()
    count = 0

    try:
        while True:
            listing = generate_listing()
            key = listing["listing"]["city"]
            value = json.dumps(listing, ensure_ascii=False)

            producer.produce(
                topic=TOPIC,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_callback,
            )

            count += 1
            city = listing["listing"]["city"]
            price = listing["listing"]["price"]
            print(f"[{count}] {city}: {price:,} ILS")

            producer.poll(0)  # Trigger delivery callbacks
            time.sleep(random.uniform(1, 5))  # 1-5 seconds between listings

    except KeyboardInterrupt:
        print(f"\nStopping... Produced {count} listings.")
    finally:
        producer.flush()  # Wait for all messages to be delivered
        print("Producer closed.")


if __name__ == "__main__":
    main()