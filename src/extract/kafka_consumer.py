"""
Kafka Consumer - Real Estate Listings

Consumes listing events from Kafka topics and processes them
for storage in the data warehouse.

Topics consumed:
- new_listings: New property listings
- price_updates: Price changes on existing listings
"""

import json
import logging
from datetime import datetime
from typing import Callable, Optional, List

logger = logging.getLogger(__name__)

DEFAULT_BOOTSTRAP_SERVERS = "localhost:29092"
TOPIC_NEW_LISTINGS = "new_listings"
TOPIC_PRICE_UPDATES = "price_updates"
CONSUMER_GROUP = "real-estate-pipeline"


class ListingConsumer:
    """Kafka consumer for real estate listing events."""

    def __init__(
        self,
        bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
        group_id: str = CONSUMER_GROUP,
        topics: Optional[List[str]] = None,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics or [TOPIC_NEW_LISTINGS, TOPIC_PRICE_UPDATES]
        self._consumer = None
        self._handlers = {
            "new_listing": [],
            "price_update": [],
        }

    def _get_consumer(self):
        """Lazy-initialize Kafka consumer."""
        if self._consumer is None:
            from confluent_kafka import Consumer
            self._consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
            })
            self._consumer.subscribe(self.topics)
            logger.info(f"Subscribed to topics: {self.topics}")
        return self._consumer

    def register_handler(self, event_type: str, handler: Callable):
        """
        Register a handler function for an event type.

        Args:
            event_type: 'new_listing' or 'price_update'
            handler: Callable that takes a dict (event data)
        """
        if event_type in self._handlers:
            self._handlers[event_type].append(handler)
            logger.info(f"Registered handler for '{event_type}': {handler.__name__}")
        else:
            raise ValueError(f"Unknown event type: {event_type}")

    def _process_message(self, message_value: str):
        """Parse and route a message to registered handlers."""
        try:
            data = json.loads(message_value)
            event_type = data.get("event_type", "")

            handlers = self._handlers.get(event_type, [])
            if not handlers:
                logger.warning(f"No handlers for event type: {event_type}")
                return

            for handler in handlers:
                try:
                    handler(data)
                except Exception as e:
                    logger.error(
                        f"Handler {handler.__name__} failed for "
                        f"event {event_type}: {e}"
                    )
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse message: {e}")

    def consume(
        self,
        max_messages: Optional[int] = None,
        timeout_seconds: float = 1.0,
    ):
        """
        Start consuming messages.

        Args:
            max_messages: Stop after N messages (None = run forever).
            timeout_seconds: Poll timeout in seconds.
        """
        consumer = self._get_consumer()
        count = 0

        logger.info("Starting consumer loop...")

        try:
            while True:
                if max_messages and count >= max_messages:
                    break

                msg = consumer.poll(timeout=timeout_seconds)

                if msg is None:
                    continue
                if msg.error():
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

                value = msg.value().decode('utf-8')
                logger.debug(
                    f"Received message from {msg.topic()} "
                    f"[{msg.partition()}] at offset {msg.offset()}"
                )

                self._process_message(value)
                count += 1

        except KeyboardInterrupt:
            logger.info("Consumer stopped by user")
        finally:
            consumer.close()
            logger.info(f"Total messages consumed: {count}")

    def get_consumer_lag(self) -> dict:
        """
        Get consumer lag information.

        Returns:
            Dictionary with topic partition lag info.
        """
        consumer = self._get_consumer()
        lag_info = {}

        try:
            from confluent_kafka import TopicPartition
            for topic in self.topics:
                partitions = consumer.assignment()
                for tp in partitions:
                    if tp.topic == topic:
                        committed = consumer.committed([tp])
                        low, high = consumer.get_watermark_offsets(tp)
                        current = committed[0].offset if committed else 0
                        lag_info[f"{topic}:{tp.partition}"] = {
                            "current_offset": current,
                            "high_watermark": high,
                            "lag": high - current if current >= 0 else high,
                        }
        except Exception as e:
            logger.warning(f"Failed to get consumer lag: {e}")

        return lag_info

    def close(self):
        """Close the consumer."""
        if self._consumer:
            self._consumer.close()
            self._consumer = None


# ============================================
# Default Handlers
# ============================================

def log_new_listing(data: dict):
    """Default handler: log new listings."""
    logger.info(
        f"New listing: {data.get('city')} | "
        f"{data.get('rooms')} rooms | "
        f"{data.get('price', 0):,} ILS"
    )


def log_price_update(data: dict):
    """Default handler: log price updates."""
    logger.info(
        f"Price update: {data.get('listing_id')} | "
        f"{data.get('old_price', 0):,} -> {data.get('new_price', 0):,} "
        f"({data.get('change_percent', 0):+.1f}%)"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    consumer = ListingConsumer()
    consumer.register_handler("new_listing", log_new_listing)
    consumer.register_handler("price_update", log_price_update)

    print("Starting consumer (Ctrl+C to stop)...")
    consumer.consume()
