"""Airflow DAG for metadata-driven ETL."""

import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def run_etl_task():
    """Execute the ETL process."""
    from etl_engine.processor import run_etl
    
    # Path to metadata file
    metadata_path = Path("/usr/local/airflow/dags/config/metadata.json")
    
    run_etl(str(metadata_path))


# DAG configuration
default_args = {
    'owner': 'Jesus Saenz',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='technical_test_etl',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 10, 1),
    catchup=False,
) as dag:
    
    # Single task: run ETL
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl_task,
    )