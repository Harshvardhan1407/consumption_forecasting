from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from datetime import datetime, timedelta
from logger_file import logger  # Assuming logger.py is in the same directory
from etl.ingestion import read_parquet  # Assuming ingestion.py is in the same directory
import pandas as pd
# from data.dir import filepath  # Assuming file_path.py is in the same directory
def extract():
    try:
        df = read_parquet(file_path= r"data\jpdcl_hes_lf_dataset.parquet")
        print(f"Extracted DataFrame shape: {df.shape}")
        print("#"*250)
        logger.info(f"Extracted DataFrame shape: {df.shape}")
        # logger.info(filepath)
    except Exception as e:
        logger.error(f"Error in extraction: {e}", exc_info=True)
        return pd.dataframe()  # Return an empty DataFrame on error
def transform():
    print("Transforming data...")

def load():
    print("Loading data...")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='sample_etl_dag',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval='@daily',  # runs once a day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    task_extract = PythonOperator(
        task_id='extract',
        python_callable=extract,
    )

    task_transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    task_load = PythonOperator(
        task_id='load',
        python_callable=load,
    )

    # Define task dependencies
    task_extract >> task_transform >> task_load
