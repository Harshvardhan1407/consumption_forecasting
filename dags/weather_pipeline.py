# # from airflow import DAG 
# # from airflow.operators.python import PythonOperator 
# # from airflow.decorators import dag, task # type: ignore
# # from airflow.utils.dates import days_ago # type: ignore
# import os
# import requests
# import pandas as pd
# from datetime import datetime, timedelta
# from logger_file import logger  # Assuming logger.py is in the same directory
# from config.db_config import get_mongo_connection
# from config.mongo_queries import jpdcl_sensor_query, jpdcl_sensor_query_projection

# from dotenv import load_dotenv
# load_dotenv()

# db = get_mongo_connection()
# collection = os.getenv("jpdcl_sensor_collection")

# def jpdcl_site_data(db, mongo_collection, query, projection):
#     try:        
#         sensor_data = list(db[mongo_collection].find(query, projection))
#         sensor_df = pd.DataFrame(sensor_data)

#         logger.info(f"Sensor data: {sensor_df.shape}")
#         sensor_df = sensor_df[sensor_df['admin_status'] != 'N']
#         logger.info(f"Sensor data after filtering:\n{sensor_df.isna().sum()}")
#         return sensor_df

#     except Exception as e:
#         logger.error(f"Error in jpdcl_site_data: {e}")
#         raise e
    

# def weather_data_api(latitude, longitude, from_date, to_date, duration="hour"):
#     try:
#         url = (
#             f"https://archive-api.open-meteo.com/v1/archive?"
#             f"latitude={latitude}&longitude={longitude}"
#             f"&start_date={from_date}&end_date={to_date}"
#             "&hourly=temperature_2m,relative_humidity_2m,apparent_temperature,"
#             "precipitation,rain,wind_speed_10m,wind_speed_100m"
#         )
#         response = requests.get(url, timeout=10)
#         response.raise_for_status()
#         weather_data = response.json()

#         weather_df = pd.DataFrame({
#             'time': weather_data['hourly']['time'],
#             'apparent_temperature': weather_data['hourly']['apparent_temperature'],
#             'rain': weather_data['hourly']['rain'],
#             'wind_speed_10m': weather_data['hourly']['wind_speed_10m'],
#             'relative_humidity_2m': weather_data['hourly']['relative_humidity_2m']
#         })

#         weather_df['time'] = pd.to_datetime(weather_df['time'])
#         weather_df.rename(columns={"time": "creation_time"}, inplace=True)
#         weather_df.set_index("creation_time", inplace=True)

#         if duration != "hour":
#             # Add 30 minutes of future data (copy of last row)
#             next_time = weather_df.index[-1] + pd.Timedelta(minutes=30)
#             last_row = weather_df.iloc[[-1]].copy()
#             last_row.index = [next_time]
#             weather_df = pd.concat([weather_df, last_row])

#             weather_resampled_df = weather_df.resample(rule=duration).asfreq()
#             weather_resampled_df = weather_resampled_df.interpolate(method="linear")

#             return weather_resampled_df

#         return weather_df

#     except Exception as e:
#         print(f"Failed to fetch weather for lat={latitude}, lon={longitude}: {e}")
#         return pd.DataFrame()


# # sensor_df = jpdcl_site_data(db, collection, jpdcl_sensor_query, jpdcl_sensor_query_projection)

# # if sensor_df.empty:

