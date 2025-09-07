from airflow import DAG
from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import os

URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet" 
OUTPUT_PATH = "/opt/airflow/data/yellow_tripdata_2025-01.parquet"

default_args = { 
    "owner": "airflow", 
    "retries": 1,
}

def ingest_to_postgres(): 
    import ingest_script
    ingester = ingest_script.DataIngestion()
    return ingester.ingestion()

with DAG(
    dag_id="dag_ingest_param",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=True,
    description="Ingestion DAG wtih dynamic date-based filenames",
    tags=["parametrized", "templating"]
    ) as dag:

    download_data = BashOperator(task_id="download_data", 
                                 bash_command=(
                                     "curl -sS -o /opt/airflow/data/yellow_tripdata_{{ ds[:7] }}.parquet " "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{{ ds[:7] }}.parquet"
                                     )
                                )

    # Optional ingestion step 
    ingest_data = PythonOperator(task_id="ingest_data_to_pg",
                                 python_callable=ingest_to_postgres()
                                 )
    
    download_data >> ingest_data    