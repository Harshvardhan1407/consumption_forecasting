from datetime import datetime, timedelta
from logger_file import logger  # Assuming logger.py is in the same directory
from etl.ingestion import read_parquet_file  # Assuming ingestion.py is in the same directory
import pandas as pd
import numpy as np
from etl.transformation import sensor_data_handling, basic_dataframe_checks, process_location_wrapper
import os
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
        directory_path1 = config.etl_config.complete_data_file_path
        directory_path2 = config.etl_config.sensor_data_file_path
        logger.info(f"Reading parquet file from: {directory_path1} and {directory_path2}")
         # Ensure the directory exists
        df = read_parquet_file(file_path=directory_path1)
        sensor_df = read_parquet_file(file_path=directory_path2)
        return df, sensor_df
    except Exception as e:
        logger.error(f"Error in extraction: {e}", exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on error

def transform(df,sensor_df):
    try:
        logger.info("Transformation start")
        sensor_df = sensor_data_handling(sensor_df)
        if sensor_df.empty:
            logger.warning("Sensor data is empty after processing. Skipping transformation.")
            return pd.DataFrame()
        
        # merge the main DataFrame with sensor data on 'location_id'
        load_df_with_sensor_data = df.merge(sensor_df, on='location_id', how='left')
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
    
if __name__ == "__main__":
    df, sensor_df = extract()
    if not df.empty and not sensor_df.empty:
        transformed_data = transform(df, sensor_df)
        # print(transformed_data.head())
    else:
        logger.warning("Data extraction failed. Skipping transformation.")