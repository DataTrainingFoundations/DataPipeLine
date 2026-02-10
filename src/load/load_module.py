import pandas as pd
import logging
from sqlalchemy import text
from src.db.engine import get_engine

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.engine = get_engine()

    @staticmethod
    def map_dtype_to_pg(dtype):
        if "int" in str(dtype):
            return "BIGINT"
        elif "float" in str(dtype):
            return "DOUBLE PRECISION"
        elif "bool" in str(dtype):
            return "BOOLEAN"
        elif "datetime" in str(dtype):
            return "TIMESTAMP"
        else:
            return "TEXT"


    def create_(self, df: pd.DataFrame, table_name: str, primary_key: str = "id"):
        if df is None or df.empty:
            logger.error(f"Cannot create table '{table_name}': DataFrame is empty or None")
            raise ValueError("DataFrame is empty or None")

        columns = []

        # Add DataFrame columns Primary Key passed in
        for col, dtype in zip(df.columns, df.dtypes):
            sql_type = self.map_dtype_to_pg(dtype)

            if col == primary_key:
                columns.append(f"{col} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col} {sql_type}")

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            raise


    def merge_(self, df: pd.DataFrame, table_name: str, primary_key: str):
        if df is None or df.empty:
            logger.error(f"Cannot upsert into '{table_name}': DataFrame is empty or None")
            raise ValueError("DataFrame is empty or None")

        columns = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in columns])

        update_clause = ", ".join(
            [f"{c}=VALUES({c})" for c in columns if c != primary_key]
        )
        sql = f""" INSERT INTO {table_name} ({', '.join(columns)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
        """

        records = df.to_dict(orient="records")

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql), records)
            logger.info(f"Upserted {len(df)} rows into '{table_name}' successfully.")
        except Exception as e:
            logger.error(f"Failed to upsert rows into '{table_name}': {e}")
            raise


    def drop_(self, table_name: str):
        try:
            with self.engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
            logger.info(f"Table '{table_name}' dropped successfully.")
        except Exception as e:
            logger.error(f"Failed to drop table '{table_name}': {e}")
            raise

