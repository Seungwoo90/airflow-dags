import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def log_details(*args, **kwargs):
    logging.info(f"My execution date is {kwargs['ds']}")
    logging.info(f"My execution date is {kwargs['execution_date']}")

dag = DAG(
    'lesson1.demo5',
    schedule_interval="@daily",
    start_date=datetime.datetime.now() - datetime.timedelta(days=2)
    )

list_task = PythonOperator(
    task_id="log_details",
    python_callable=log_details,
    dag=dag,
    provide_context=True
    )
