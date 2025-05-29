import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # project root

complete_data_file_path = os.path.join(BASE_DIR, 'data', 'ingestion_data_dir', 'HT_meter_complete_data_9_id.parquet')
sensor_data_file_path = os.path.join(BASE_DIR, 'data', 'ingestion_data_dir', 'sensor_data.parquet')

mongo_url = "mongodb://35.154.221.2:27017/"
jpdcl_db = "jpdcl"
jpdcl_loadprofile_2024 = "loadprofile_AMI_FDR_MDM_2024"
jpdcl_loadprofile_2025 = "loadprofile_AMI_FDR_MDM_2025"
jpdcl_sensor_collection = "sensor"

# data_file_path = r"data/ingestion_data_dir/HT_meter_complete_data_9_id.parquet"
# data_file_path = r"/data/ingestion_data_dir/HT_meter_complete_data_9_id.parquet"