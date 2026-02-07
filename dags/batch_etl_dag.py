"""
Batch ETL Pipeline DAG

Daily pipeline: extract real estate data -> store raw -> transform -> load -> quality checks -> analytics.
Schedule: Daily at 06:00
"""

from datetime import datetime, timedelta
import logging
import sys
import os

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'real-estate-roi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@task()
def extract_gov_data(days_back: int = 30):
    """Extract real estate price trends from nadlan.gov.il CloudFront API."""
    from extract.gov_api import fetch_all_settlements_trends

    logger.info(f"Starting extraction for all known settlements")
    trends = fetch_all_settlements_trends()
    logger.info(f"Extracted {len(trends)} trend records")
    return {"record_count": len(trends), "trends": trends}


@task()
def store_raw_to_minio(extraction_result: dict):
    """Store raw extracted data to Minio S3 storage."""
    from load.minio_client import MinioStorageClient

    logger.info("Storing raw data to Minio...")
    client = MinioStorageClient()

    raw_path = client.upload_json(
        data=extraction_result["trends"],
        path="raw/gov_trends/trends.json",
    )

    logger.info(f"Stored raw data to: {raw_path}")
    return {
        "raw_path": raw_path,
        "record_count": extraction_result["record_count"],
        "trends": extraction_result["trends"],
    }


@task()
def transform_data(minio_result: dict):
    """Transform and enrich trend data."""
    from transform.address_normalizer import normalize_city

    logger.info("Transforming data...")
    trends = minio_result["trends"]

    for trend in trends:
        raw_city = trend.get('settlementName', '')
        trend['normalized_city'] = normalize_city(raw_city)

    logger.info(f"Transformed {len(trends)} records")
    return trends


@task()
def load_to_warehouse(trends: list):
    """Load transformed data to PostgreSQL data warehouse."""
    from load.postgres_loader import PostgresLoader

    logger.info("Loading to data warehouse...")
    loader = PostgresLoader()
    loader.load_price_trends(trends)

    stats = {"total": len(trends), "inserted": len(trends), "errors": 0}
    logger.info(f"Load complete: {stats}")
    return stats


@task()
def run_quality_checks(load_stats: dict):
    """Run data quality checks."""
    logger.info("Running data quality checks...")

    checks_passed = True
    messages = []

    if load_stats.get("errors", 0) > load_stats.get("total", 0) * 0.1:
        checks_passed = False
        messages.append(f"Error rate too high: {load_stats['errors']}/{load_stats['total']}")

    if load_stats.get("inserted", 0) == 0 and load_stats.get("total", 0) > 0:
        checks_passed = False
        messages.append("No records were inserted")

    result = {"passed": checks_passed, "messages": messages, "load_stats": load_stats}
    if not checks_passed:
        logger.warning(f"Quality checks failed: {messages}")
    else:
        logger.info("Quality checks passed")
    return result


@task()
def refresh_analytics(quality_result: dict):
    """Refresh ROI analytics metrics."""
    from load.postgres_loader import PostgresLoader

    if not quality_result["passed"]:
        logger.warning("Skipping analytics refresh due to quality check failures")
        return {"skipped": True, "reason": quality_result["messages"]}

    logger.info("Refreshing analytics metrics...")
    loader = PostgresLoader()
    updated_count = loader.refresh_roi_metrics()
    logger.info(f"Updated {updated_count} location metrics")
    return {"updated_count": updated_count}


with DAG(
    dag_id='batch_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for real estate price trends',
    schedule_interval='0 6 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'etl', 'batch'],
    doc_md="""
    ## Batch ETL Pipeline

    Daily pipeline that fetches real estate price trends from nadlan.gov.il,
    stores raw data in Minio, transforms, loads to PostgreSQL, runs quality
    checks, and refreshes analytics.

    **Schedule**: Daily at 06:00
    """,
) as dag:

    extraction = extract_gov_data(days_back=30)
    raw_storage = store_raw_to_minio(extraction)
    transformed = transform_data(raw_storage)
    loaded = load_to_warehouse(transformed)
    quality = run_quality_checks(loaded)
    analytics = refresh_analytics(quality)
