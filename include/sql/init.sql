-- ============================================
-- The Smart Nadlanist — Star Schema
-- PostgreSQL + PostGIS
-- ============================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================
-- DIMENSION TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS dim_location (
    location_id     SERIAL PRIMARY KEY,
    city            VARCHAR(100) NOT NULL,
    neighborhood    VARCHAR(100),
    street          VARCHAR(150),
    district        VARCHAR(50),
    latitude        DECIMAL(10, 7),
    longitude       DECIMAL(10, 7),
    geom            GEOMETRY(POINT, 4326),
    created_at      TIMESTAMP DEFAULT NOW(),
    UNIQUE(city, neighborhood, street)
);

CREATE TABLE IF NOT EXISTS dim_property (
    property_id     SERIAL PRIMARY KEY,
    property_type   VARCHAR(50),
    rooms           DECIMAL(3,1),
    year_built      INT,
    floor           INT,
    total_floors    INT,
    has_elevator    BOOLEAN,
    has_parking     BOOLEAN,
    has_balcony     BOOLEAN,
    created_at      TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_time (
    time_id         INT PRIMARY KEY,
    full_date       DATE NOT NULL,
    year            INT NOT NULL,
    quarter         INT NOT NULL,
    month           INT NOT NULL,
    month_name      VARCHAR(20),
    day_of_week     INT,
    is_post_covid   BOOLEAN DEFAULT FALSE,
    is_war_period   BOOLEAN DEFAULT FALSE,
    UNIQUE(full_date)
);

INSERT INTO dim_time (time_id, full_date, year, quarter, month, month_name, day_of_week, is_post_covid, is_war_period)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT AS time_id,
    d AS full_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    CASE EXTRACT(MONTH FROM d)::INT
        WHEN 1 THEN 'January' WHEN 2 THEN 'February' WHEN 3 THEN 'March'
        WHEN 4 THEN 'April' WHEN 5 THEN 'May' WHEN 6 THEN 'June'
        WHEN 7 THEN 'July' WHEN 8 THEN 'August' WHEN 9 THEN 'September'
        WHEN 10 THEN 'October' WHEN 11 THEN 'November' WHEN 12 THEN 'December'
    END AS month_name,
    EXTRACT(DOW FROM d)::INT AS day_of_week,
    d >= '2020-03-01' AS is_post_covid,
    d >= '2023-10-07' AS is_war_period
FROM generate_series('2015-01-01'::DATE, '2030-12-31'::DATE, '1 day') AS d
ON CONFLICT (full_date) DO NOTHING;


CREATE TABLE IF NOT EXISTS dim_macro_economic (
    macro_id            SERIAL PRIMARY KEY,
    time_id             INT REFERENCES dim_time(time_id),
    boi_interest_rate   DECIMAL(5,2),
    avg_mortgage_rate   DECIMAL(5,2),
    usd_ils_rate        DECIMAL(6,3),
    cpi_index           DECIMAL(8,2),
    construction_starts INT,
    created_at          TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- FACT TABLES
-- ============================================

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id      SERIAL PRIMARY KEY,
    location_id         INT REFERENCES dim_location(location_id),
    property_id         INT REFERENCES dim_property(property_id),
    time_id             INT REFERENCES dim_time(time_id),
    price               DECIMAL(12,0) NOT NULL,
    sqm                 DECIMAL(8,2),
    price_per_sqm       DECIMAL(10,0),
    transaction_type    VARCHAR(20),
    gush                VARCHAR(20),
    helka               VARCHAR(20),
    boi_rate_at_sale    DECIMAL(5,2),
    cbs_index_at_sale   DECIMAL(8,2),
    source              VARCHAR(50) DEFAULT 'nadlan.gov.il',
    raw_address         TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_market_indices (
    index_id                SERIAL PRIMARY KEY,
    time_id                 INT REFERENCES dim_time(time_id),
    location_id             INT REFERENCES dim_location(location_id),
    cbs_price_index         DECIMAL(8,2),
    avg_price_per_sqm       DECIMAL(10,0),
    construction_input_idx  DECIMAL(8,2),
    boi_interest_rate       DECIMAL(5,2),
    created_at              TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS fact_roi_analysis (
    roi_id                  SERIAL PRIMARY KEY,
    location_id             INT REFERENCES dim_location(location_id),
    time_id                 INT REFERENCES dim_time(time_id),
    avg_price_per_sqm       DECIMAL(10,0),
    price_appreciation_1yr  DECIMAL(6,2),
    price_appreciation_5yr  DECIMAL(6,2),
    estimated_rental_yield  DECIMAL(5,2),
    roi_score               DECIMAL(4,1),
    distance_to_train_km    DECIMAL(5,2),
    distance_to_school_km   DECIMAL(5,2),
    nearby_amenities_count  INT,
    created_at              TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- STAGING TABLE
-- ============================================

CREATE TABLE IF NOT EXISTS stg_nadlan_raw (
    id              SERIAL PRIMARY KEY,
    raw_json        JSONB NOT NULL,
    source          VARCHAR(50),
    ingested_at     TIMESTAMP DEFAULT NOW(),
    is_processed    BOOLEAN DEFAULT FALSE
);

-- ============================================
-- INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_transactions_location ON fact_transactions(location_id);
CREATE INDEX IF NOT EXISTS idx_transactions_time ON fact_transactions(time_id);
CREATE INDEX IF NOT EXISTS idx_transactions_price ON fact_transactions(price);
CREATE INDEX IF NOT EXISTS idx_location_city ON dim_location(city);
CREATE INDEX IF NOT EXISTS idx_location_geom ON dim_location USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_roi_score ON fact_roi_analysis(roi_score DESC);
CREATE INDEX IF NOT EXISTS idx_roi_location ON fact_roi_analysis(location_id);
