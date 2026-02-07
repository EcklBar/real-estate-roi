"""
Kafka Producer - Simulated Real Estate Listings

Generates realistic listing events based on real market trend data
from nadlan.gov.il and publishes them to Kafka topics.

Topics:
- new_listings: New property listings
- price_updates: Price changes on existing listings
"""

import json
import logging
import random
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# Default Kafka configuration
DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NEW_LISTINGS = "new_listings"
TOPIC_PRICE_UPDATES = "price_updates"

# Realistic data for simulation
CITIES = [
    {"name": "תל אביב -יפו", "id": 5000, "avg_price_sqm": 45000, "lat": 32.0853, "lon": 34.7818},
    {"name": "ירושלים", "id": 3000, "avg_price_sqm": 32000, "lat": 31.7683, "lon": 35.2137},
    {"name": "חיפה", "id": 4000, "avg_price_sqm": 20000, "lat": 32.7940, "lon": 34.9896},
    {"name": "באר שבע", "id": 9000, "avg_price_sqm": 14000, "lat": 31.2530, "lon": 34.7915},
    {"name": "ראשון לציון", "id": 8300, "avg_price_sqm": 28000, "lat": 31.9730, "lon": 34.7925},
    {"name": "פתח תקווה", "id": 7900, "avg_price_sqm": 26000, "lat": 32.0841, "lon": 34.8878},
    {"name": "נתניה", "id": 7400, "avg_price_sqm": 24000, "lat": 32.3215, "lon": 34.8532},
    {"name": "הרצליה", "id": 6400, "avg_price_sqm": 42000, "lat": 32.1629, "lon": 34.7914},
    {"name": "רעננה", "id": 8700, "avg_price_sqm": 35000, "lat": 32.1849, "lon": 34.8706},
    {"name": "רמת גן", "id": 8600, "avg_price_sqm": 33000, "lat": 32.0700, "lon": 34.8243},
]

PROPERTY_TYPES = ["דירה", "דירת גן", "פנטהאוז", "דירת גג", "בית פרטי", "דופלקס"]
STREETS_SAMPLE = [
    "הרצל", "רוטשילד", "דיזנגוף", "אבן גבירול", "ז'בוטינסקי",
    "ויצמן", "סוקולוב", "ביאליק", "בן יהודה", "אלנבי",
    "שדרות ירושלים", "הנשיא", "הרב קוק", "גורדון", "פינסקר",
]


def generate_listing() -> dict:
    """Generate a single realistic property listing event."""
    city = random.choice(CITIES)
    rooms = random.choice([2, 2.5, 3, 3.5, 4, 4.5, 5, 5.5, 6])
    size_sqm = int(rooms * random.uniform(18, 30))
    floor = random.randint(0, 20)
    year_built = random.randint(1960, 2025)

    # Price based on city average with variance
    price_variance = random.uniform(0.7, 1.4)
    price_per_sqm = int(city["avg_price_sqm"] * price_variance)
    price = price_per_sqm * size_sqm

    # Round price to nearest 10,000
    price = round(price / 10000) * 10000

    return {
        "event_type": "new_listing",
        "listing_id": f"LST-{random.randint(100000, 999999)}",
        "timestamp": datetime.now().isoformat(),
        "city": city["name"],
        "city_id": city["id"],
        "street": random.choice(STREETS_SAMPLE),
        "house_number": str(random.randint(1, 200)),
        "property_type": random.choice(PROPERTY_TYPES),
        "rooms": rooms,
        "floor": floor,
        "total_floors": max(floor, random.randint(floor, floor + 10)),
        "size_sqm": size_sqm,
        "year_built": year_built,
        "price": price,
        "price_per_sqm": price_per_sqm,
        "lat": city["lat"] + random.uniform(-0.03, 0.03),
        "lon": city["lon"] + random.uniform(-0.03, 0.03),
    }


def generate_price_update(listing: dict) -> dict:
    """Generate a price update event for an existing listing."""
    change_pct = random.uniform(-0.10, 0.05)  # -10% to +5%
    new_price = int(listing["price"] * (1 + change_pct))
    new_price = round(new_price / 10000) * 10000

    return {
        "event_type": "price_update",
        "listing_id": listing["listing_id"],
        "timestamp": datetime.now().isoformat(),
        "city": listing["city"],
        "old_price": listing["price"],
        "new_price": new_price,
        "change_percent": round(change_pct * 100, 1),
    }


class ListingProducer:
    """Kafka producer for simulated real estate listing events."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
    ):
        self.bootstrap_servers = bootstrap_servers
        self._producer = None

    def _get_producer(self):
        """Lazy-initialize Kafka producer."""
        if self._producer is None:
            from confluent_kafka import Producer
            self._producer = Producer({
                'bootstrap.servers': self.bootstrap_servers,
                'client.id': 'real-estate-listing-producer',
            })
        return self._producer

    def _delivery_callback(self, err, msg):
        """Callback for message delivery reports."""
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Message delivered to {msg.topic()} [{msg.partition()}] "
                f"at offset {msg.offset()}"
            )

    def produce_listing(self, listing: Optional[dict] = None):
        """Produce a single listing event to Kafka."""
        if listing is None:
            listing = generate_listing()

        producer = self._get_producer()
        producer.produce(
            topic=TOPIC_NEW_LISTINGS,
            key=listing["listing_id"],
            value=json.dumps(listing, ensure_ascii=False),
            callback=self._delivery_callback,
        )
        producer.poll(0)
        return listing

    def produce_price_update(self, listing: dict):
        """Produce a price update event to Kafka."""
        update = generate_price_update(listing)
        producer = self._get_producer()
        producer.produce(
            topic=TOPIC_PRICE_UPDATES,
            key=update["listing_id"],
            value=json.dumps(update, ensure_ascii=False),
            callback=self._delivery_callback,
        )
        producer.poll(0)
        return update

    def run_simulation(
        self,
        listings_per_minute: int = 5,
        duration_minutes: Optional[int] = None,
        price_update_chance: float = 0.2,
    ):
        """
        Run continuous listing simulation.

        Args:
            listings_per_minute: Number of new listings per minute.
            duration_minutes: How long to run (None = forever).
            price_update_chance: Probability of generating a price update.
        """
        logger.info(
            f"Starting simulation: {listings_per_minute} listings/min, "
            f"duration={duration_minutes or 'infinite'} min"
        )

        active_listings = []
        start_time = time.time()
        interval = 60.0 / listings_per_minute
        count = 0

        try:
            while True:
                # Check duration
                if duration_minutes:
                    elapsed = (time.time() - start_time) / 60
                    if elapsed >= duration_minutes:
                        break

                # Produce new listing
                listing = self.produce_listing()
                active_listings.append(listing)
                count += 1
                logger.info(
                    f"[{count}] New listing: {listing['city']} - "
                    f"{listing['rooms']} rooms - "
                    f"{listing['price']:,} ILS"
                )

                # Maybe produce price update
                if active_listings and random.random() < price_update_chance:
                    old_listing = random.choice(active_listings)
                    update = self.produce_price_update(old_listing)
                    logger.info(
                        f"  Price update: {update['listing_id']} "
                        f"{update['old_price']:,} -> {update['new_price']:,} "
                        f"({update['change_percent']:+.1f}%)"
                    )

                # Flush
                self._get_producer().flush(timeout=5)

                # Keep list manageable
                if len(active_listings) > 100:
                    active_listings = active_listings[-50:]

                time.sleep(interval)

        except KeyboardInterrupt:
            logger.info("Simulation stopped by user")
        finally:
            if self._producer:
                self._producer.flush(timeout=10)
            logger.info(f"Total listings produced: {count}")

    def close(self):
        """Flush and close the producer."""
        if self._producer:
            self._producer.flush(timeout=10)
            self._producer = None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    # Demo: generate sample listings without Kafka
    print("=== Sample Generated Listings ===\n")
    for i in range(5):
        listing = generate_listing()
        print(
            f"{i + 1}. {listing['city']} | {listing['street']} {listing['house_number']} | "
            f"{listing['rooms']} rooms | {listing['size_sqm']}sqm | "
            f"{listing['price']:,} ILS ({listing['price_per_sqm']:,}/sqm)"
        )

    print("\n=== Sample Price Update ===\n")
    sample = generate_listing()
    update = generate_price_update(sample)
    print(
        f"Listing {update['listing_id']}: "
        f"{update['old_price']:,} -> {update['new_price']:,} "
        f"({update['change_percent']:+.1f}%)"
    )
