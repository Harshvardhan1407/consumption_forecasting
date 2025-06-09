from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import os

from datetime import datetime, timedelta
from logger_file import logger  # Assuming logger.py is in the same directory
from etl.ingestion import read_parquet_file  # Assuming ingestion.py is in the same directory
import pandas as pd
import numpy as np
from etl.transformation import sensor_data_handling, basic_dataframe_checks, process_location_wrapper
import config.etl_config 
import threading
import time
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed, ThreadPoolExecutor
import multiprocessing
# plt.rcParams["figure.figsize"]=14,5
pd.set_option('display.max_columns', None)
pd.set_option('display.float_format', '{:.6f}'.format)  # Show 6 decimal places

def extract():
    try:
        pass
    except Exception as e:
        logger.error(f"Error in extraction: {e}", exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on error

def transform():
    try:
        directory_path1 = config.etl_config.complete_data_file_path
        directory_path2 = config.etl_config.sensor_data_file_path
        logger.info(f"Reading parquet file from: {directory_path1} and {directory_path2}")
         # Ensure the directory exists
        df = read_parquet_file(file_path=directory_path1)
        sensor_df = read_parquet_file(file_path=directory_path2)
        
        logger.info("Transformation start")
        sensor_df = sensor_data_handling(sensor_df)
        if sensor_df.empty:
            logger.warning("Sensor data is empty after processing. Skipping transformation.")
            return pd.DataFrame()
        # merge the main DataFrame with sensor data on 'location_id'
        try:
            logger.info(f"data merging started with shape: {df.columns} and sensor_df shape: {sensor_df.columns}")
            load_df_with_sensor_data = df.merge(sensor_df, on='location_id', how='left')
            logger.info(f"data merged with shape: {load_df_with_sensor_data.shape}")
        except KeyError as e:
            logger.error(f"KeyError during merge: {e}", exc_info=True)
            return pd.DataFrame()


        load_df_with_sensor_data.dropna(inplace=True)
        logger.info("data merged  successfully")
        # perform basic checks on the merged DataFrame
        basic_dataframe_checks(load_df_with_sensor_data)

        # group by 'location_id' and process each group in parallel
        location_groups = [(id, data.copy()) for id, data in load_df_with_sensor_data.groupby("location_id")]
        logger.info(f"Number of location groups: {len(location_groups)}")
        
        # Set max_workers = number of CPU cores or as desired
        max_workers = min(4, multiprocessing.cpu_count())  # Adjust 4 if needed
        results = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(process_location_wrapper, group) for group in location_groups[0:11]]

            for future in as_completed(futures):
                result = future.result()
                if not result.empty:
                    results.append(result)

        # Combine all results
        clean_data_set = pd.concat(results, ignore_index=False)
        logger.info(clean_data_set.shape)
    
    except Exception as e:
        logger.error(f"Error in transformation: {e}", exc_info=True)
        return pd.DataFrame()

def load():
    print("Loading data...")

default_args = {
    'owner': 'airflow',
    'retries': 0,
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
