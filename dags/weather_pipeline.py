# from airflow import DAG 
# from airflow.operators.python import PythonOperator 
# from airflow.decorators import dag, task # type: ignore
# from airflow.utils.dates import days_ago # type: ignore

import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from logger_file import logger  # Assuming logger.py is in the same directory
from config.db_config import get_mongo_connection

db = get_mongo_connection()


sensor_data = list(db[f"{os.getenv('jpdcl_collection_3')}"].find({},{
                    "_id": 0,
                    "location_id":1,
                    "site_id": 1,
                    "latitude": 1,
                    "longitude": 1
                    }))


# @dag(
#     schedule_interval='@monthly',
#     start_date=datetime(2025, 4, 14),
#     catchup=False,
#     tags=["weather", "monthly", "etl"]
# )
# def monthly_weather_etl():

#     @task
#     def load_sites_from_mongodb():

#         sites = list(db[f"{os.getenv("sensor")}"].find({}, {"_id": 0, "site_id": 1, "lat": 1, "lon": 1}))
#         return sites

#     @task
#     def fetch_weather(site):
#         lat = site['lat']
#         lon = site['lon']
#         try:
#             response = requests.get(
#                 f"https://api.weatherapi.com/v1/current.json",
#                 params={"key": API_KEY, "q": f"{lat},{lon}"},
#                 timeout=10
#             )
#             data = response.json()
#             return {
#                 "site_id": site['site_id'],
#                 "weather": data
#             }
#         except Exception as e:
#             return {"site_id": site['site_id'], "error": str(e)}

#     @task
#     def store_weather(data):
#         client = MongoClient(MONGO_URI)
#         db = client[DB_NAME]
#         collection = db["weather_data"]
#         collection.insert_one(data)

#     sites = load_sites_from_mongodb()
#     weather_data = fetch_weather.expand(site=sites)
#     store_weather.expand(data=weather_data)

# dag = monthly_weather_etl()