"""
PostgreSQL Data Warehouse Loader

Handles loading transformed data into the PostgreSQL + PostGIS
data warehouse using upsert (INSERT ON CONFLICT) patterns.
"""

import logging
import os
from typing import List, Dict, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Default connection parameters (overridable via env vars)
DEFAULT_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5433")),
    "dbname": os.getenv("POSTGRES_DB", "real_estate_dw"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
}


class PostgresLoader:
    """Loads data into the real estate data warehouse."""

    def __init__(self, config: Optional[Dict] = None):
        self.config = config or DEFAULT_CONFIG
        self._conn = None

    def _get_connection(self):
        """Get or create database connection."""
        if self._conn is None or self._conn.closed:
            self._conn = psycopg2.connect(**self.config)
            self._conn.autocommit = False
        return self._conn

    def close(self):
        """Close database connection."""
        if self._conn and not self._conn.closed:
            self._conn.close()
            self._conn = None

    def init_schema(self, sql_file: str = "include/sql/create_tables.sql"):
        """Initialize database schema from SQL file."""
        conn = self._get_connection()
        try:
            with open(sql_file, 'r') as f:
                sql = f.read()
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            logger.info("Schema initialized successfully")
        except Exception as e:
            conn.rollback()
            logger.error(f"Failed to initialize schema: {e}")
            raise

    # ============================================
    # Dimension: Locations
    # ============================================

    def upsert_location(self, record: dict) -> int:
        """
        Upsert a location and return its ID.

        Args:
            record: Dict with city, neighborhood, street, lat, lon, distances.

        Returns:
            location_id from dim_locations.
        """
        conn = self._get_connection()
        sql = """
            INSERT INTO dim_locations (
                city, neighborhood, street, settlement_id, neighborhood_id,
                lat, lon, geom,
                dist_to_train_km, dist_to_school_km, dist_to_park_km
            ) VALUES (
                %(city)s, %(neighborhood)s, %(street)s,
                %(settlement_id)s, %(neighborhood_id)s,
                %(lat)s, %(lon)s,
                CASE WHEN %(lat)s IS NOT NULL AND %(lon)s IS NOT NULL
                     THEN ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326)
                     ELSE NULL END,
                %(dist_to_train_km)s, %(dist_to_school_km)s, %(dist_to_park_km)s
            )
            ON CONFLICT (city, neighborhood, street)
            DO UPDATE SET
                lat = COALESCE(EXCLUDED.lat, dim_locations.lat),
                lon = COALESCE(EXCLUDED.lon, dim_locations.lon),
                geom = COALESCE(EXCLUDED.geom, dim_locations.geom),
                dist_to_train_km = COALESCE(EXCLUDED.dist_to_train_km, dim_locations.dist_to_train_km),
                dist_to_school_km = COALESCE(EXCLUDED.dist_to_school_km, dim_locations.dist_to_school_km),
                dist_to_park_km = COALESCE(EXCLUDED.dist_to_park_km, dim_locations.dist_to_park_km)
            RETURNING location_id
        """
        with conn.cursor() as cur:
            cur.execute(sql, {
                'city': record.get('normalized_city') or record.get('city', ''),
                'neighborhood': record.get('neighborhood', ''),
                'street': record.get('normalized_street') or record.get('street', ''),
                'settlement_id': record.get('settlement_id'),
                'neighborhood_id': record.get('neighborhood_id'),
                'lat': record.get('lat'),
                'lon': record.get('lon'),
                'dist_to_train_km': record.get('dist_to_train_stations_km'),
                'dist_to_school_km': record.get('dist_to_schools_km'),
                'dist_to_park_km': record.get('dist_to_parks_km'),
            })
            return cur.fetchone()[0]

    # ============================================
    # Dimension: Properties
    # ============================================

    def upsert_property(self, record: dict) -> int:
        """
        Upsert a property and return its ID.

        Args:
            record: Dict with property details.

        Returns:
            property_id from dim_properties.
        """
        conn = self._get_connection()
        sql = """
            INSERT INTO dim_properties (
                gush_helka, property_type, rooms, floor,
                total_floors, size_sqm, year_built, is_new_project
            ) VALUES (
                %(gush_helka)s, %(property_type)s, %(rooms)s, %(floor)s,
                %(total_floors)s, %(size_sqm)s, %(year_built)s, %(is_new_project)s
            )
            ON CONFLICT (gush_helka, rooms, floor, size_sqm)
            DO UPDATE SET
                property_type = COALESCE(EXCLUDED.property_type, dim_properties.property_type),
                total_floors = COALESCE(EXCLUDED.total_floors, dim_properties.total_floors),
                year_built = COALESCE(EXCLUDED.year_built, dim_properties.year_built)
            RETURNING property_id
        """
        with conn.cursor() as cur:
            cur.execute(sql, {
                'gush_helka': record.get('gush_helka', ''),
                'property_type': record.get('property_type', ''),
                'rooms': record.get('rooms'),
                'floor': record.get('floor'),
                'total_floors': record.get('total_floors'),
                'size_sqm': record.get('size_sqm'),
                'year_built': record.get('year_built'),
                'is_new_project': record.get('is_new_project', False),
            })
            return cur.fetchone()[0]

    # ============================================
    # Fact: Transactions
    # ============================================

    def load_transaction(self, record: dict, location_id: int, property_id: int):
        """Load a single transaction into fact_transactions."""
        conn = self._get_connection()
        sql = """
            INSERT INTO fact_transactions (
                location_id, property_id, source_id,
                transaction_date, price, price_per_sqm, deal_nature,
                estimated_rent, rental_yield_pct, roi_score, scraped_at
            ) VALUES (
                %(location_id)s, %(property_id)s, %(source_id)s,
                %(transaction_date)s, %(price)s, %(price_per_sqm)s,
                %(deal_nature)s, %(estimated_rent)s, %(rental_yield_pct)s,
                %(roi_score)s, %(scraped_at)s
            )
            ON CONFLICT (source_id) DO NOTHING
        """
        with conn.cursor() as cur:
            cur.execute(sql, {
                'location_id': location_id,
                'property_id': property_id,
                'source_id': record.get('source_id', ''),
                'transaction_date': record.get('transaction_date'),
                'price': record.get('price'),
                'price_per_sqm': record.get('price_per_sqm'),
                'deal_nature': record.get('deal_nature', ''),
                'estimated_rent': record.get('estimated_monthly_rent'),
                'rental_yield_pct': record.get('rental_yield_pct'),
                'roi_score': record.get('roi_score'),
                'scraped_at': record.get('scraped_at'),
            })

    # ============================================
    # Batch Loading
    # ============================================

    def load_transactions_batch(self, records: List[dict]) -> dict:
        """
        Load a batch of enriched transaction records.

        Args:
            records: List of enriched transaction dicts.

        Returns:
            Statistics dict with counts.
        """
        conn = self._get_connection()
        stats = {"total": len(records), "inserted": 0, "errors": 0, "skipped": 0}

        for record in records:
            try:
                location_id = self.upsert_location(record)
                property_id = self.upsert_property(record)
                self.load_transaction(record, location_id, property_id)
                stats["inserted"] += 1
            except Exception as e:
                logger.warning(f"Failed to load record: {e}")
                stats["errors"] += 1
                conn.rollback()
                conn = self._get_connection()

        try:
            conn.commit()
        except Exception as e:
            logger.error(f"Commit failed: {e}")
            conn.rollback()

        logger.info(
            f"Batch load complete: {stats['inserted']}/{stats['total']} inserted, "
            f"{stats['errors']} errors"
        )
        return stats

    # ============================================
    # Price Trends Loading
    # ============================================

    def load_price_trends(self, trends: List[dict]):
        """
        Load price trend data into agg_price_trends.

        Args:
            trends: List of trend records from gov_api.extract_price_trends().
        """
        conn = self._get_connection()
        sql = """
            INSERT INTO agg_price_trends (
                settlement_id, settlement_name,
                neighborhood_id, neighborhood_name,
                trend_year, trend_month, num_rooms,
                settlement_price, country_price, neighborhood_price
            ) VALUES (
                %(settlement_id)s, %(settlement_name)s,
                %(neighborhood_id)s, %(neighborhood_name)s,
                %(year)s, %(month)s, %(numRooms)s,
                %(settlementPrice)s, %(countryPrice)s, %(neighborhoodPrice)s
            )
            ON CONFLICT (settlement_id, neighborhood_id, trend_year, trend_month, num_rooms)
            DO UPDATE SET
                settlement_price = EXCLUDED.settlement_price,
                country_price = EXCLUDED.country_price,
                neighborhood_price = EXCLUDED.neighborhood_price,
                fetched_at = CURRENT_TIMESTAMP
        """
        count = 0
        for trend in trends:
            try:
                with conn.cursor() as cur:
                    cur.execute(sql, {
                        'settlement_id': trend.get('settlementID', 0),
                        'settlement_name': trend.get('settlementName', ''),
                        'neighborhood_id': trend.get('neighborhoodID', 0),
                        'neighborhood_name': trend.get('neighborhoodName', ''),
                        'year': trend.get('year'),
                        'month': trend.get('month'),
                        'numRooms': str(trend.get('numRooms', 'all')),
                        'settlementPrice': trend.get('settlementPrice'),
                        'countryPrice': trend.get('countryPrice'),
                        'neighborhoodPrice': trend.get('neighborhoodPrice'),
                    })
                    count += 1
            except Exception as e:
                logger.warning(f"Failed to load trend: {e}")
                conn.rollback()
                conn = self._get_connection()

        conn.commit()
        logger.info(f"Loaded {count} price trend records")

    # ============================================
    # Analytics Refresh
    # ============================================

    def refresh_roi_metrics(self) -> int:
        """
        Refresh aggregated ROI metrics from fact_transactions.

        Returns:
            Number of metrics updated.
        """
        conn = self._get_connection()
        sql = """
            INSERT INTO agg_roi_metrics (
                city, neighborhood, period_year, period_quarter,
                avg_price, median_price, avg_price_per_sqm,
                min_price, max_price, deal_count,
                avg_rental_yield, avg_roi_score
            )
            SELECT
                l.city,
                l.neighborhood,
                EXTRACT(YEAR FROM t.transaction_date)::INTEGER AS period_year,
                EXTRACT(QUARTER FROM t.transaction_date)::INTEGER AS period_quarter,
                AVG(t.price)::BIGINT,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.price)::BIGINT,
                AVG(t.price_per_sqm),
                MIN(t.price),
                MAX(t.price),
                COUNT(*),
                AVG(t.rental_yield_pct),
                AVG(t.roi_score)
            FROM fact_transactions t
            JOIN dim_locations l ON t.location_id = l.location_id
            WHERE t.transaction_date IS NOT NULL
              AND t.price > 0
            GROUP BY l.city, l.neighborhood,
                     EXTRACT(YEAR FROM t.transaction_date),
                     EXTRACT(QUARTER FROM t.transaction_date)
            ON CONFLICT (city, neighborhood, period_year, period_quarter)
            DO UPDATE SET
                avg_price = EXCLUDED.avg_price,
                median_price = EXCLUDED.median_price,
                avg_price_per_sqm = EXCLUDED.avg_price_per_sqm,
                min_price = EXCLUDED.min_price,
                max_price = EXCLUDED.max_price,
                deal_count = EXCLUDED.deal_count,
                avg_rental_yield = EXCLUDED.avg_rental_yield,
                avg_roi_score = EXCLUDED.avg_roi_score,
                calculated_at = CURRENT_TIMESTAMP
        """
        with conn.cursor() as cur:
            cur.execute(sql)
            updated = cur.rowcount
        conn.commit()
        logger.info(f"Refreshed {updated} ROI metrics")
        return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("PostgresLoader ready. Use with docker-compose PostgreSQL.")
    print(f"Default config: {DEFAULT_CONFIG}")
