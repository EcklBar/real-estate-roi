#!/usr/bin/env python3
"""
Create stream_listings table and indexes if they don't exist.
Run when the warehouse was created before we added this table to init.sql.
Usage: python scripts/init_stream_listings.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv

load_dotenv()

WAREHOUSE_HOST = os.getenv("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = os.getenv("WAREHOUSE_PORT", "5433")
WAREHOUSE_DB = os.getenv("WAREHOUSE_DB", "nadlanist")
WAREHOUSE_USER = os.getenv("WAREHOUSE_USER")
WAREHOUSE_PASSWORD = os.getenv("WAREHOUSE_PASSWORD")

DDL = """
CREATE TABLE IF NOT EXISTS stream_listings (
    id              SERIAL PRIMARY KEY,
    source          VARCHAR(50) NOT NULL,
    listing_id      VARCHAR(100) NOT NULL,
    city            VARCHAR(100),
    neighborhood    VARCHAR(100),
    street          VARCHAR(150),
    rooms           DECIMAL(3,1),
    sqm             INT,
    floor           INT,
    price           DECIMAL(12,0) NOT NULL,
    price_per_sqm   DECIMAL(10,0),
    property_type   VARCHAR(50),
    url             TEXT,
    event_time      TIMESTAMPTZ,
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(source, listing_id)
);
CREATE INDEX IF NOT EXISTS idx_stream_listings_source ON stream_listings(source);
CREATE INDEX IF NOT EXISTS idx_stream_listings_city ON stream_listings(city);
CREATE INDEX IF NOT EXISTS idx_stream_listings_price ON stream_listings(price);
CREATE INDEX IF NOT EXISTS idx_stream_listings_created ON stream_listings(created_at DESC);
"""


def main():
    if not WAREHOUSE_USER or not WAREHOUSE_PASSWORD:
        print("Set WAREHOUSE_USER and WAREHOUSE_PASSWORD in .env")
        sys.exit(1)
    try:
        import psycopg2
        conn = psycopg2.connect(
            host=WAREHOUSE_HOST,
            port=WAREHOUSE_PORT,
            dbname=WAREHOUSE_DB,
            user=WAREHOUSE_USER,
            password=WAREHOUSE_PASSWORD,
        )
        cur = conn.cursor()
        cur.execute(DDL)
        conn.commit()
        cur.close()
        conn.close()
        print("stream_listings table and indexes created successfully.")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
