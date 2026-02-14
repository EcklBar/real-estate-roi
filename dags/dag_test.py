from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def say_hello(**context):
    print("Hello from Nadlanist Airflow!")
    
    
with DAG(
    dag_id="test_hello",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule=None,
    tags=["test"],
) as dag:
    
    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )