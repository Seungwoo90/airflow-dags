import datetime
import logging
import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("Hello world")

def current_time():
    logging.info(f"Current time is {datetime.datetime.utcnow().isoformat()}")

def working_dir():
    logging.info(f"Working directory is {os.getcwd()}")

def complete():
    logging.info("Congrats, your fist multi-task pipeline is now complete!")


dag = DAG(
    "lesson1.demo3",
    schedule_interval='@hourly',
    start_date=datetime.datetime.now() - datetime.timedelta(days=1))

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)

current_time = PythonOperator(
    task_id="current_time",
    python_callable=current_time,
    dag=dag)

working_dir = PythonOperator(
    task_id="working_dir",
    python_callable=working_dir,
    dag=dag)

complete_task = PythonOperator(
    task_id="complete",
    python_callable=complete,
    dag=dag)

hello_world_task >> current_time
hello_world_task >> working_dir
current_time >> complete_task
working_dir >> complete_task

