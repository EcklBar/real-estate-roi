"""
Streaming Monitor DAG

Monitors Kafka streaming pipeline health every 15 minutes.
Checks consumer lag, processes pending messages, and generates alerts.
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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


@task()
def check_kafka_health():
    """Check Kafka broker connectivity and topic status."""
    logger.info("Checking Kafka health...")

    try:
        from confluent_kafka.admin import AdminClient
        admin = AdminClient({'bootstrap.servers': 'kafka:9092'})
        topics = admin.list_topics(timeout=10)

        topic_names = list(topics.topics.keys())
        logger.info(f"Kafka healthy. Topics: {topic_names}")

        return {
            "healthy": True,
            "topic_count": len(topic_names),
            "topics": topic_names,
        }
    except Exception as e:
        logger.error(f"Kafka health check failed: {e}")
        return {"healthy": False, "error": str(e)}


@task()
def check_consumer_lag(kafka_status: dict):
    """Check consumer group lag."""
    if not kafka_status.get("healthy"):
        logger.warning("Skipping lag check - Kafka unhealthy")
        return {"skipped": True}

    logger.info("Checking consumer lag...")
    try:
        from extract.kafka_consumer import ListingConsumer
        consumer = ListingConsumer(bootstrap_servers='kafka:9092')
        lag = consumer.get_consumer_lag()
        consumer.close()

        total_lag = sum(v.get('lag', 0) for v in lag.values())
        logger.info(f"Total consumer lag: {total_lag}")

        return {"lag_info": lag, "total_lag": total_lag}
    except Exception as e:
        logger.warning(f"Failed to check consumer lag: {e}")
        return {"error": str(e), "total_lag": -1}


@task()
def process_pending_messages(lag_info: dict):
    """Process any pending messages from Kafka."""
    if lag_info.get("skipped") or lag_info.get("total_lag", 0) == 0:
        logger.info("No pending messages to process")
        return {"processed": 0}

    logger.info("Processing pending messages...")
    try:
        from extract.kafka_consumer import ListingConsumer, log_new_listing, log_price_update

        consumer = ListingConsumer(bootstrap_servers='kafka:9092')
        consumer.register_handler("new_listing", log_new_listing)
        consumer.register_handler("price_update", log_price_update)

        # Process up to 100 messages
        consumer.consume(max_messages=100, timeout_seconds=5.0)
        consumer.close()

        return {"processed": min(100, lag_info.get("total_lag", 0))}
    except Exception as e:
        logger.error(f"Failed to process messages: {e}")
        return {"error": str(e), "processed": 0}


@task()
def generate_alerts(process_result: dict):
    """Generate alerts if needed."""
    logger.info("Checking for alerts...")
    alerts = []

    processed = process_result.get("processed", 0)
    if processed > 50:
        alerts.append(f"High volume: {processed} messages processed")

    if process_result.get("error"):
        alerts.append(f"Processing error: {process_result['error']}")

    if alerts:
        logger.warning(f"Alerts generated: {alerts}")
    else:
        logger.info("No alerts")

    return {"alert_count": len(alerts), "alerts": alerts}


with DAG(
    dag_id='streaming_monitor',
    default_args=default_args,
    description='Monitor Kafka streaming pipeline health',
    schedule_interval='*/15 * * * *',
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'streaming', 'monitoring'],
    doc_md="""
    ## Streaming Monitor

    Monitors the Kafka streaming pipeline every 15 minutes.
    Checks broker health, consumer lag, processes pending messages,
    and generates alerts.
    """,
) as dag:

    kafka_health = check_kafka_health()
    lag = check_consumer_lag(kafka_health)
    processed = process_pending_messages(lag)
    alerts = generate_alerts(processed)
