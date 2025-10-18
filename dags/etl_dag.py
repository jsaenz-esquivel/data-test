"""Airflow DAG for metadata-driven ETL with data generation."""

import sys
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add paths to Python
sys.path.insert(0, '/opt/airflow') 

from airflow import DAG
from airflow.operators.python import PythonOperator

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def generate_new_data():
    """Generate new incremental data before ETL run."""
    import json
    import random
    from faker import Faker
    
    # Configuration
    NUM_NEW_RECORDS = 200
    INVALID_PERCENTAGE = 0.25
    OUTPUT_DIR = Path("/data/input/events/person")
    OFFICES = ["MADRID", "BARCELONA", "VALENCIA", "SEVILLA", "BILBAO", "ZARAGOZA"]
    
    fake = Faker('es_ES')
    
    # Get next file number
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        start_num = 1
    else:
        existing = list(OUTPUT_DIR.glob("person_*.json"))
        if not existing:
            start_num = 1
        else:
            numbers = []
            for f in existing:
                try:
                    num = int(f.stem.split('_')[1])
                    numbers.append(num)
                except (IndexError, ValueError):
                    continue
            start_num = max(numbers) + 1 if numbers else 1
    
    end_num = start_num + NUM_NEW_RECORDS
    
    logging.info(f"Generating {NUM_NEW_RECORDS} new records...")
    logging.info(f"File range: person_{start_num:04d}.json to person_{end_num-1:04d}.json")
    
    # Generate records
    num_invalid = int(NUM_NEW_RECORDS * INVALID_PERCENTAGE)
    invalid_indices = random.sample(range(NUM_NEW_RECORDS), num_invalid)
    
    for i in range(NUM_NEW_RECORDS):
        force_invalid = i in invalid_indices
        
        # Generate record
        record = {
            "name": fake.name(),
            "age": random.randint(22, 65),
            "office": random.choice(OFFICES)
        }
        
        if force_invalid:
            error_type = random.choice(["missing_age", "empty_office", "both"])
            if error_type == "missing_age":
                del record["age"]
            elif error_type == "empty_office":
                record["office"] = ""
            else:
                del record["age"]
                record["office"] = ""
        
        # Write file
        file_num = start_num + i
        filename = OUTPUT_DIR / f"person_{file_num:04d}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(record, f, ensure_ascii=False, indent=4)
    
    total_files = len(list(OUTPUT_DIR.glob("person_*.json")))
    logging.info(f"✓ Generated {NUM_NEW_RECORDS} new files")
    logging.info(f"✓ Total files now: {total_files}")


def run_etl_task():
    """Execute the ETL process."""
    from etl_engine.processor import run_etl
    
    metadata_path = Path("/opt/airflow/dags/config/metadata.json")
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
    schedule='*/15 * * * *',  # Every 15 minutes
    start_date=datetime(2025, 10, 18),
    catchup=False,
) as dag:
    
    # Task 1: Generate new data
    generate_data_task = PythonOperator(
        task_id='generate_incremental_data',
        python_callable=generate_new_data,
    )
    
    # Task 2: Run ETL
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=run_etl_task,
    )
    
    # Task dependency
    generate_data_task >> etl_task