-- ============================================
-- Real Estate ROI - Data Warehouse Schema
-- Star Schema with PostGIS geospatial support
-- ============================================

-- Enable PostGIS extension
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================
-- DIMENSION: Locations
-- ============================================

CREATE TABLE IF NOT EXISTS dim_locations (
    location_id         SERIAL PRIMARY KEY,
    city                VARCHAR(100) NOT NULL,
    neighborhood        VARCHAR(200),
    street              VARCHAR(200),
    settlement_id       INTEGER,
    neighborhood_id     BIGINT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    geom                GEOMETRY(Point, 4326),

    -- Distances to amenities (km)
    dist_to_train_km    DOUBLE PRECISION,
    dist_to_school_km   DOUBLE PRECISION,
    dist_to_park_km     DOUBLE PRECISION,

    -- Metadata
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (city, neighborhood, street)
);

CREATE INDEX IF NOT EXISTS idx_dim_locations_city
    ON dim_locations (city);
CREATE INDEX IF NOT EXISTS idx_dim_locations_neighborhood
    ON dim_locations (neighborhood);
CREATE INDEX IF NOT EXISTS idx_dim_locations_geom
    ON dim_locations USING GIST (geom);

-- ============================================
-- DIMENSION: Properties
-- ============================================

CREATE TABLE IF NOT EXISTS dim_properties (
    property_id         SERIAL PRIMARY KEY,
    gush_helka          VARCHAR(50),
    property_type       VARCHAR(100),
    rooms               REAL,
    floor               INTEGER,
    total_floors        INTEGER,
    size_sqm            REAL,
    year_built          INTEGER,
    is_new_project      BOOLEAN DEFAULT FALSE,

    -- Metadata
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (gush_helka, rooms, floor, size_sqm)
);

CREATE INDEX IF NOT EXISTS idx_dim_properties_type
    ON dim_properties (property_type);
CREATE INDEX IF NOT EXISTS idx_dim_properties_rooms
    ON dim_properties (rooms);

-- ============================================
-- FACT: Transactions (Historical deals)
-- ============================================

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      SERIAL PRIMARY KEY,
    location_id         INTEGER REFERENCES dim_locations(location_id),
    property_id         INTEGER REFERENCES dim_properties(property_id),
    source_id           VARCHAR(200),

    -- Transaction details
    transaction_date    DATE,
    price               BIGINT,
    price_per_sqm       REAL,
    deal_nature         VARCHAR(200),

    -- Calculated fields
    estimated_rent      REAL,
    rental_yield_pct    REAL,
    roi_score           REAL,

    -- Metadata
    scraped_at          TIMESTAMP,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (source_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_tx_date
    ON fact_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_fact_tx_location
    ON fact_transactions (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_tx_price
    ON fact_transactions (price);

-- ============================================
-- FACT: Listings (Streaming - current)
-- ============================================

CREATE TABLE IF NOT EXISTS fact_listings (
    listing_id          VARCHAR(50) PRIMARY KEY,
    location_id         INTEGER REFERENCES dim_locations(location_id),
    property_id         INTEGER REFERENCES dim_properties(property_id),

    -- Listing details
    price               BIGINT,
    price_per_sqm       REAL,
    listing_date        TIMESTAMP,
    is_active           BOOLEAN DEFAULT TRUE,

    -- Price history
    original_price      BIGINT,
    price_change_pct    REAL DEFAULT 0,

    -- ROI metrics
    estimated_rent      REAL,
    rental_yield_pct    REAL,
    roi_score           REAL,

    -- Metadata
    kafka_offset        BIGINT,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_fact_listings_active
    ON fact_listings (is_active);
CREATE INDEX IF NOT EXISTS idx_fact_listings_score
    ON fact_listings (roi_score);

-- ============================================
-- AGGREGATE: ROI Metrics
-- ============================================

CREATE TABLE IF NOT EXISTS agg_roi_metrics (
    metric_id           SERIAL PRIMARY KEY,
    city                VARCHAR(100) NOT NULL,
    neighborhood        VARCHAR(200),

    -- Period
    period_year         INTEGER NOT NULL,
    period_quarter      INTEGER NOT NULL,

    -- Price metrics
    avg_price           BIGINT,
    median_price        BIGINT,
    avg_price_per_sqm   REAL,
    min_price           BIGINT,
    max_price           BIGINT,

    -- Volume metrics
    deal_count          INTEGER DEFAULT 0,

    -- Trend metrics
    price_change_pct    REAL,       -- QoQ change
    yoy_change_pct      REAL,       -- YoY change

    -- ROI metrics
    avg_rental_yield    REAL,
    avg_roi_score       REAL,

    -- Metadata
    calculated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (city, neighborhood, period_year, period_quarter)
);

CREATE INDEX IF NOT EXISTS idx_agg_roi_city
    ON agg_roi_metrics (city);
CREATE INDEX IF NOT EXISTS idx_agg_roi_period
    ON agg_roi_metrics (period_year, period_quarter);

-- ============================================
-- AGGREGATE: Price Trends (from CloudFront API)
-- ============================================

CREATE TABLE IF NOT EXISTS agg_price_trends (
    trend_id            SERIAL PRIMARY KEY,
    settlement_id       INTEGER NOT NULL,
    settlement_name     VARCHAR(100),
    neighborhood_id     BIGINT,
    neighborhood_name   VARCHAR(200),

    -- Period
    trend_year          INTEGER NOT NULL,
    trend_month         INTEGER NOT NULL,
    num_rooms           VARCHAR(10),    -- '3', '4', '5', 'all'

    -- Prices
    settlement_price    BIGINT,
    country_price       BIGINT,
    neighborhood_price  BIGINT,

    -- Metadata
    fetched_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (settlement_id, neighborhood_id, trend_year, trend_month, num_rooms)
);

CREATE INDEX IF NOT EXISTS idx_trends_settlement
    ON agg_price_trends (settlement_id);
CREATE INDEX IF NOT EXISTS idx_trends_period
    ON agg_price_trends (trend_year, trend_month);

-- ============================================
-- Helper function: Update timestamp trigger
-- ============================================

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_dim_locations_updated
    BEFORE UPDATE ON dim_locations
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_dim_properties_updated
    BEFORE UPDATE ON dim_properties
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();

CREATE TRIGGER trg_fact_listings_updated
    BEFORE UPDATE ON fact_listings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at();
