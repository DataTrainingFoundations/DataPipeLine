import pandas as pd
from sql_queries_module import sql_insert, sql_update
from DBConnection_Module import DbConnector


class DataLoader:

    def __init__(self):
        self.db = DbConnector()

    def load_csv_to_Database(self, df, table_name: str):
        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")
        
        conn = self.db.connect()
        cursor = conn.cursor()
        _insert = sql_queries.sql_insert
        try:
            data = pd.read_csv(file_path, dtype=str)
            return data
        except Exception as e:
            print(f"Error loading CSV file: {e}")
            return None