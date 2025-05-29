import pandas as pd
# from config.db_config import db
from logger_file import logger 
import os

def read_parquet_file(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file_path)
        logger.info(f"DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        logger.error(f"Error reading parquet file: {e}",exc_info=True)
        return pd.DataFrame()  # Return an empty DataFrame on error


"""
{
 '0:0:1:0:0:255': 'Clock',
 '1:0:32:27:0:255': 'L1VoltageAvg',
 '1:0:52:27:0:255': 'L2VoltageAvg',
 '1:0:72:27:0:255': 'L3VoltageAvg',
 '1:0:31:27:0:255': 'L1CurrentAvg',
 '1:0:51:27:0:255': 'L2CurrentAvg',
 '1:0:71:27:0:255': 'L3CurrentAvg',
 '1:0:1:29:0:255': 'BlockEnergy-WhImp',
 '1:0:9:29:0:255': 'BlockEnergy-VAhImp',
 }
 """

def get_data_from_mongo(db):
    # Fetch data from MongoDB
    data = list(db[os.getenv("jpdcl_collection")].find({},{
        "_id":0,
        '0:0:1:0:0:255': 1,
        '1:0:32:27:0:255': 1,
        '1:0:52:27:0:255': 1,
        '1:0:72:27:0:255': 1,
        '1:0:31:27:0:255': 1,
        '1:0:51:27:0:255': 1,
        '1:0:71:27:0:255': 1,
        '1:0:1:29:0:255': 1,
        '1:0:9:29:0:255': 1,
        "location_id":1,
        "serial_no":1,
    }))
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    
    return df

# complete_data = list(collection.find({},{
#     "_id":0,
# 	'1:0:31:27:0:255': 1,
# 	'1:0:51:27:0:255': 1,
# 	'1:0:71:27:0:255': 1,
# 	'1:0:32:27:0:255': 1,
# 	'1:0:52:27:0:255': 1,
# 	'1:0:72:27:0:255': 1,
# 	'0:0:1:0:0:255': 1,
# 	'1:0:1:29:0:255': 1,
# 	'1:0:9:29:0:255': 1,
# 	"serial_no":1,
# 	"location_id":1,
# }))
# complete_data