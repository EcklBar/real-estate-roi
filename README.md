# Real Estate ROI Predictor

A comprehensive data engineering pipeline for identifying real estate investment opportunities in Israel.

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-2.9-green.svg)
![Kafka](https://img.shields.io/badge/Kafka-7.5-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)

## Project Overview

This project implements a **Multi-Purpose Data Pipeline System** that:
- Acquires real estate data from **nadlan.gov.il** (batch) and simulated listings (streaming)
- Processes and enriches data with **geographic information** from OpenStreetMap
- Calculates **ROI metrics** and investment scores
- Visualizes opportunities through an **interactive dashboard**

### Business Value
Investors can use this system to identify high-ROI real estate opportunities based on:
- Historical transaction prices and price appreciation trends
- Rental yield calculations
- Proximity to amenities (train stations, schools, parks)
- Market liquidity

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                                   │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│  nadlan.gov.il API  │   Kafka Stream      │    OpenStreetMap API            │
│  (Batch - Daily)    │   (Real-time)       │    (Enrichment)                │
└──────────┬──────────┴──────────┬──────────┴───────────────┬─────────────────┘
           │                    │                           │
           ▼                    ▼                           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    MINIO (S3-Compatible Storage)                            │
│                  Partitioned by: year/month/day                             │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORM LAYER (Python)                                 │
│    • Address Normalization (Hebrew)  • Geocoding  • Feature Engineering     │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                 DATA WAREHOUSE (PostgreSQL + PostGIS)                       │
│       Star Schema: dim_locations, dim_properties, fact_transactions        │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ORCHESTRATION (Apache Airflow)                          │
│    DAG 1: batch_etl_pipeline (daily)                                       │
│    DAG 2: streaming_monitor (every 15 min)                                 │
│    DAG 3: analytics_pipeline (daily)                                       │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                   PRESENTATION (Streamlit Dashboard)                        │
│           Interactive maps, ROI charts, investment alerts                   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Technologies

| Category | Technology | Purpose |
|----------|------------|---------|
| **Orchestration** | Apache Airflow | DAG scheduling, workflow management |
| **Streaming** | Apache Kafka | Real-time data ingestion |
| **Storage** | Minio (S3) | Partitioned raw/processed data |
| **Database** | PostgreSQL + PostGIS | Data warehouse with geospatial |
| **Processing** | Python / Pandas | Data transformations |
| **Dashboard** | Streamlit + Folium + Plotly | Interactive visualization |
| **Monitoring** | ELK Stack | Logging and monitoring (optional) |
| **Containerization** | Docker Compose | Full stack deployment |

---

## Project Structure

```
real-estate-roi/
├── dags/                          # Airflow DAGs
│   ├── batch_etl_dag.py           # Daily batch ETL
│   ├── streaming_monitor_dag.py   # Kafka monitoring
│   └── analytics_dag.py          # ROI calculation
├── src/
│   ├── extract/
│   │   ├── gov_api.py             # nadlan.gov.il API client + scraper
│   │   ├── kafka_producer.py      # Kafka producer (simulated listings)
│   │   ├── kafka_consumer.py      # Kafka consumer
│   │   └── osm_api.py            # OpenStreetMap API
│   ├── transform/
│   │   ├── address_normalizer.py  # Hebrew address normalization
│   │   └── feature_engineering.py # ROI calculations
│   └── load/
│       ├── minio_client.py        # S3 storage client
│       └── postgres_loader.py     # Data warehouse loader
├── dashboard/
│   ├── app.py                     # Streamlit main app
│   └── components/
│       ├── map_view.py            # Folium maps
│       └── charts.py             # Plotly charts
├── include/
│   └── sql/
│       └── create_tables.sql      # Database schema (star schema)
├── docker/
│   └── postgres/
│       └── init-postgis.sh        # PostGIS initialization
├── docker-compose.yaml            # Full stack definition
├── .env.example                   # Environment variables template
├── requirements.txt               # Python dependencies
└── README.md
```

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- 8GB+ RAM recommended

### 1. Clone and Setup

```bash
cd real-estate-roi

# Create environment file
cp .env.example .env
# Edit .env with your passwords
```

### 2. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Minio Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark UI** | http://localhost:8081 | - |

### 4. Run the Pipeline

```bash
# Trigger batch ETL from Airflow UI, or:
docker exec airflow-scheduler airflow dags trigger batch_etl_pipeline
```

### 5. Launch Dashboard

```bash
pip install streamlit plotly folium streamlit-folium

cd dashboard
streamlit run app.py
```

---

## Data Pipeline Details

### Batch ETL Pipeline (Daily at 06:00)

```
extract_gov_data -> store_raw_to_minio -> transform_data -> load_to_warehouse -> quality_checks -> refresh_analytics
```

1. **Extract**: Fetch price trends from nadlan.gov.il CloudFront API
2. **Store**: Save raw JSON to Minio with date partitioning
3. **Transform**: Normalize Hebrew addresses
4. **Load**: Upsert to PostgreSQL
5. **Quality**: Run data validation checks
6. **Analytics**: Refresh ROI metrics

### Streaming Pipeline (Every 15 min)

```
check_kafka_health -> check_consumer_lag -> process_messages -> generate_alerts
```

### Analytics Pipeline (Daily at 08:00)

```
fetch_amenities -> fetch_price_trends -> calculate_roi -> save_metrics -> generate_investment_alerts
```

---

## Data Model

### Star Schema

**Dimension Tables:**
- `dim_locations` - Cities, neighborhoods, coordinates, distances to amenities
- `dim_properties` - Property types, rooms, size, year built

**Fact Tables:**
- `fact_transactions` - Historical transaction data
- `fact_listings` - Current listings (streaming)

**Aggregates:**
- `agg_roi_metrics` - Calculated ROI scores, price trends, yields
- `agg_price_trends` - Price trend data from nadlan.gov.il

---

## Key Features

### Hebrew Address Normalization
Handles inconsistent Israeli address formats:
- `"ת"א"` -> `"תל אביב יפו"`
- `"רח' הרצל"` -> `"הרצל"`
- `"ב"ש"` -> `"באר שבע"`

### ROI Score Calculation (1-100)
Composite score based on:
- **Price benchmark** (30%) - Price vs market average
- **Appreciation** (25%) - Price growth trend
- **Rental yield** (20%) - Estimated annual yield
- **Transit proximity** (10%) - Distance to train stations
- **Amenities** (10%) - Schools, parks nearby
- **Liquidity** (5%) - Market activity level

### Investment Alerts
Automatic alerts for:
- High ROI opportunities (score > 75)
- Significant price changes
- Emerging neighborhoods

---

## Data Sources

- **nadlan.gov.il** - Israeli government real estate transaction database (CloudFront CDN API)
- **OpenStreetMap** - Geographic amenity data via Overpass API
- **Simulated listings** - Kafka-produced realistic listing events for streaming demo

---

## License

This project is part of the **Naya College Cloud Big Data Engineer** program.
