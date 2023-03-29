from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
import io

default_args = {
    'owner': 'Meghna',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 29),
    'email': ['meghna.theone@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='chicago_taxi_trip_data_pipeline',
    default_args=default_args,
    description='A data pipeline for the Chicago taxi trip dataset',
    schedule_interval='@daily',
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=data_extraction,
    dag=dag,
)

clean_dataset = PythonOperator(
    task_id='clean_dataset',
    python_callable=data_cleaning_load_to_csv,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    provide_context=True,
    python_callable=data_transformation_load_to_csv,
    dag=dag,
)

load_dataset = PythonOperator(
    task_id='load_dataset',
    provide_context=True,
    python_callable=load_data_to_snowflake,
    dag=dag,
)

# Define task dependencies
extract_data >> clean_dataset >> transform_task >> load_dataset