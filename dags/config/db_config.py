from pymongo import MongoClient
import os 
from logger_file import logger  
import dags.config.etl_config as pc
def get_mongo_connection():
    try:
        logger.info("Connecting to MongoDB...")
        client = MongoClient(pc.mongo_url, "mongodb://localhost:27017/") 
        db = client[pc.jpdcl_db]  # Replace with your database name
        logger.info(f"collections available: {db.list_collection_names()}")
        logger.info("MongoDB connection established.")
        return db
    
    except Exception as e:
        logger.error(f"Error connecting to MongoDB: {e}",exc_info=True)
        return None
    
