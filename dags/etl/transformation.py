import threading
import time
import multiprocessing as mp
import pandas as pd
from concurrent.futures import ThreadPoolExecutor , ProcessPoolExecutor
import numpy as np
from logger_file import logger

def sensor_data_handling(sensor_df):
	try:
		logger.info("Sensor data handling started")
		sensor_df = sensor_df[sensor_df['admin_status'] != "N"]
		sensor_df = sensor_df[['location_id', 'site_id', 'meter_ct_mf','meter_load_mf','meter_volt_mf']]
		logger.info("Sensor data handling started2")

		for col in ['meter_ct_mf', 'meter_load_mf', 'meter_volt_mf']:
			sensor_df[col] = pd.to_numeric(sensor_df[col], errors='coerce')
			# sensor_df[['meter_ct_mf','meter_load_mf','meter_volt_mf']] = sensor_df[['meter_ct_mf','meter_load_mf','meter_volt_mf']].astype(float)
		logger.info(f"Sensor DataFrame shape after filtering: {sensor_df.shape}")
		return sensor_df
	except Exception as e:
		logger.error(f"Error in sensor_data_handling: {e}", exc_info=True)
		return pd.DataFrame()  

def basic_dataframe_checks(df):
	logger.info(f"df rows: {len(df)}")
	logger.info(f"null values: \n{df.isnull().sum().sum()}")
	logger.info(f"Duplicated rows: {df.duplicated().sum()}")
	# logger.info(f"\ninfo: {df.info()}")
	# logger.info(f"\ndescription: {df.describe()}")

def process_location_data(df_loc):
    # Example transformations (replace with your logic)
    df_loc['R_Voltage'] *= df_loc['meter_volt_mf']
    df_loc['Y_Voltage'] *= df_loc['meter_volt_mf']
    df_loc['B_Voltage'] *= df_loc['meter_volt_mf']

    df_loc['R_Current'] *= df_loc['meter_ct_mf']
    df_loc['Y_Current'] *= df_loc['meter_ct_mf']
    df_loc['B_Current'] *= df_loc['meter_ct_mf']

    df_loc['KWh'] *= df_loc['meter_load_mf']
	
    return df_loc

def voltage_current_load_validation(df):
	try:
		non_negative_cols = ['R_Voltage', 'Y_Voltage', 'B_Voltage','R_Current', 'Y_Current', 'B_Current', 'KWh']
		voltage_cols = ['R_Voltage', 'Y_Voltage', 'B_Voltage']
		current_cols = ['R_Current', 'Y_Current', 'B_Current']

		# --- Condition 1 ---
		any_voltage_zero = (df[voltage_cols] == 0).any(axis=1)
		any_current_nonzero = (df[current_cols] != 0).any(axis=1)

		condition_1 = any_voltage_zero & any_current_nonzero

		# --- Condition 2 ---
		all_current_nonzero = (df[current_cols] != 0).all(axis=1)
		kwh_zero = (df['KWh'] == 0)

		condition_2 = all_current_nonzero & kwh_zero

		# --- Condition 3: No negatives ---
		no_negatives = (df[non_negative_cols] >= 0).all(axis=1)

		final_condition = (condition_1 | condition_2) & no_negatives
		filtered_df = df[~final_condition].copy()
		return filtered_df
		
	except Exception as e:
		logger.error(f"Error in voltage_current_load_validation: {e}")
		return pd.DataFrame()

def process_location_wrapper(group):
	# logger.info(f"Processing group: {group}")
	id, data = group
	try:
		logger.info(f"Processing location_id: {id}, {len(data)} records")
		
		clean_dataset = pre_processing(data)
		if clean_dataset.empty:
			logger.warning(f"No data after pre-processing for location_id {id}. Skipping.")
			return pd.DataFrame()
		
		resample_df = clean_dataset[['KWh']].resample('30min').asfreq()
		print(f"null vlaues after resampling: {resample_df.isna().sum().sum()}")
		resample_df = resample_df.interpolate(method="linear")
		# resampled_dataset['KWh'].plot()
		# plt.show()

		return resample_df
	except Exception as e:
		logger.error(f"Error processing location_id {id}: {e}", exc_info=True)
		return pd.DataFrame()  # Return empty DataFrame on error

def pre_processing(data):
	try:
		data['Clock'] = pd.to_datetime(data['Clock'])
		data = data.set_index("Clock", drop=True).sort_index()
		data = process_location_data(data)
		clean_dataset = voltage_current_load_validation(data)
		return clean_dataset
	except Exception as e:
		logger.error(f"Error in pre_processing: {e}", exc_info=True)
		return pd.DataFrame()