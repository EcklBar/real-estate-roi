from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, "/opt/airflow")


def run_fetch(**context):
    from src.extract.gov_api import main
    main()


def run_etl(**context):
    from processing.etl_transactions import main
    main()


default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="batch_pipeline",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule="@weekly",
    default_args=default_args,
    tags=["batch", "dirobot", "etl"],
) as dag:
    fetch_dirobot_data = PythonOperator(
        task_id="fetch_dirobot_data",
        python_callable=run_fetch,
    )
    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )
    fetch_dirobot_data >> run_etl_task