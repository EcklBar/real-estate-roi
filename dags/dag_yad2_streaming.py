from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")


def run_yad2_producer(**context):
    from src.streaming.yad2_producer import main
    main()


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="yad2_streaming",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="0 */3 * * *",
    default_args=default_args,
    tags=["streaming", "yad2", "kafka"],
) as dag:
    scrape_yad2 = PythonOperator(
        task_id="scrape_yad2",
        python_callable=run_yad2_producer,
    )
