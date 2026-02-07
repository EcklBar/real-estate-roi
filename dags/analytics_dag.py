"""
Analytics DAG

Daily analytics pipeline that enriches data with geographic information,
calculates ROI metrics, and generates investment alerts.
Schedule: Daily at 08:00
"""

from datetime import timedelta
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
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@task()
def fetch_amenities():
    """Fetch amenities from OpenStreetMap for major cities."""
    from extract.osm_api import OpenStreetMapClient, CITY_BBOXES

    logger.info("Fetching amenities from OpenStreetMap...")
    client = OpenStreetMapClient()

    all_amenities = {}
    for city, bbox in list(CITY_BBOXES.items())[:5]:  # Top 5 cities
        try:
            amenities = client.get_all_amenities_for_city(bbox)
            all_amenities[city] = {
                k: [a.to_dict() for a in v]
                for k, v in amenities.items()
            }
            logger.info(
                f"{city}: {sum(len(v) for v in amenities.values())} amenities"
            )
        except Exception as e:
            logger.warning(f"Failed to fetch amenities for {city}: {e}")

    return all_amenities


@task()
def fetch_price_trends():
    """Fetch latest price trends from nadlan.gov.il."""
    from extract.gov_api import fetch_all_settlements_trends, SETTLEMENT_CODES

    logger.info("Fetching price trends...")
    # Fetch top cities
    top_cities = list(SETTLEMENT_CODES.values())[:10]
    trends = fetch_all_settlements_trends(settlement_ids=top_cities)
    logger.info(f"Fetched {len(trends)} trend records")
    return trends


@task()
def calculate_roi(trends: list, amenities: dict):
    """Calculate ROI metrics for each location."""
    from transform.feature_engineering import (
        calculate_roi_score,
        score_appreciation_trend,
        calculate_rental_yield,
        BENCHMARK_PRICE_PER_SQM,
    )

    logger.info("Calculating ROI metrics...")
    roi_results = []

    # Group trends by settlement
    from itertools import groupby
    sorted_trends = sorted(trends, key=lambda x: x.get('settlementID', 0))

    for settlement_id, group in groupby(sorted_trends, key=lambda x: x.get('settlementID')):
        group_list = list(group)
        if not group_list:
            continue

        city = group_list[0].get('settlementName', '')
        appreciation = score_appreciation_trend(group_list)

        # Get benchmark price
        benchmark = BENCHMARK_PRICE_PER_SQM.get(city)

        roi = calculate_roi_score(
            price_per_sqm=benchmark,
            city=city,
            rental_yield=3.0,  # Default estimate
            appreciation_trends=group_list,
            deal_count=50,
        )

        roi_results.append({
            'settlement_id': settlement_id,
            'city': city,
            **roi,
        })

    logger.info(f"Calculated ROI for {len(roi_results)} locations")
    return roi_results


@task()
def save_metrics(roi_results: list):
    """Save calculated metrics to data warehouse."""
    from load.minio_client import MinioStorageClient

    logger.info("Saving ROI metrics...")
    try:
        client = MinioStorageClient()
        path = client.upload_json(
            data=roi_results,
            path="processed/roi_metrics/metrics.json",
        )
        logger.info(f"Saved metrics to: {path}")
    except Exception as e:
        logger.warning(f"Failed to save to Minio (may not be running): {e}")

    return {"saved_count": len(roi_results)}


@task()
def generate_investment_alerts(roi_results: list):
    """Generate alerts for high-ROI opportunities."""
    logger.info("Generating investment alerts...")

    alerts = []
    for result in roi_results:
        if result.get('roi_score', 0) > 70:
            alerts.append({
                "type": "high_roi",
                "city": result['city'],
                "roi_score": result['roi_score'],
                "message": (
                    f"High ROI opportunity in {result['city']}: "
                    f"score {result['roi_score']}/100"
                ),
            })

    if alerts:
        logger.info(f"Generated {len(alerts)} investment alerts")
        for alert in alerts:
            logger.info(f"  ALERT: {alert['message']}")
    else:
        logger.info("No high-ROI alerts")

    return {"alert_count": len(alerts), "alerts": alerts}


with DAG(
    dag_id='analytics_pipeline',
    default_args=default_args,
    description='Daily ROI analytics and investment alert generation',
    schedule_interval='0 8 * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'analytics', 'roi'],
    doc_md="""
    ## Analytics Pipeline

    Daily pipeline that fetches amenity data from OpenStreetMap,
    retrieves price trends, calculates ROI metrics, saves results,
    and generates investment alerts.

    **Schedule**: Daily at 08:00 (after batch ETL at 06:00)
    """,
) as dag:

    amenities = fetch_amenities()
    trends = fetch_price_trends()
    roi = calculate_roi(trends, amenities)
    saved = save_metrics(roi)
    alerts = generate_investment_alerts(roi)

    saved >> alerts
