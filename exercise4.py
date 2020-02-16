import datetime
import logging

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook

# TODO : There is no code to modify in this demo. We're going to create a connection
#  and variable.
# 1. Admin --> Variables
# 2. add Key s3_bucket, s3_prefix
# 3. Admin --> Connections
# 4. aws_credentials, Amazon Web Services, accesskey, password



def list_keys():
    hook = S3Hook(aws_conn_id='aws_credentials')
    bucket = Variable.get('s3_bucket')
    prefix = Variable.get('s3_prefix')
    logging.info(f"Listing Keys from {bucket}/{prefix}")
    keys = hook.list_keys(bucket, prefix=prefix)
    for key in keys:
        logging.info(f"- s3://{bucket}/{key}")

dag = DAG(
        "lesson1.demo4",
        start_date=datetime.datetime(2020,2,16,12,33))

list_task = PythonOperator(
    task_id="list_keys",
    python_callable=list_keys,
    dag=dag
    )
