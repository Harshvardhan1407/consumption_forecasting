import pandas as pd
from config.db_config import db
from logger_file import logger 


def read_parquet(file_path: str) -> pd.DataFrame:
    try:
        df = pd.read_parquet(file_path)
        print(f"DataFrame shape: {df.shape}")
        return df
    except Exception as e:
        print(f"Error reading parquet file: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error
