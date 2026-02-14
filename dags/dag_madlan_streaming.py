"""
DAG: Run Madlan producer (scrape madlan.co.il, publish to Kafka new_listings).
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")


def run_madlan_producer(**context):
    from src.streaming.madlan_producer import main
    main()


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="madlan_streaming",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 */4 * * *",
    default_args=default_args,
    tags=["streaming", "madlan", "kafka"],
) as dag:
    scrape_madlan = PythonOperator(
        task_id="scrape_madlan",
        python_callable=run_madlan_producer,
    )
