import pytz
import datetime
import logging
from airflow import DAG
from airflow.operators.python_operator import PythonOperator


def print_hello():
    logging.info('Hello world')
    print('This is print statements : Hello world')


dag = DAG(
        'newone',
        start_date=datetime.datetime(2020,2,15,6,5)
        )


greet_task = PythonOperator(
        task_id="greet_task",
        python_callable=print_hello,
        dag=dag
        )
