import pandas as pd
import logging
from sqlalchemy import text
from src.db.engine import get_engine

logger = logging.getLogger(__name__)


class DataLoader:
    def __init__(self):
        self.engine = get_engine()


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


    def create_table(self, df: pd.DataFrame, table_name: str, primary_key: str = "id"):
        if df is None or df.empty:
            logger.error(f"Cannot create table '{table_name}': DataFrame is empty or None")
            raise ValueError("DataFrame is empty or None")

        columns = []

        # Add auto-increment primary key column
        if primary_key:
            columns.append(f"{primary_key} BIGSERIAL PRIMARY KEY")

        # Add DataFrame columns
        for col, dtype in zip(df.columns, df.dtypes):
            if col == primary_key:
                continue  # skip if same name as PK
            col_def = f"{col} {self.map_dtype_to_pg(dtype)}"
            columns.append(col_def)

        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"Table '{table_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create table '{table_name}': {e}")
            raise


    def insert_rows(self, df: pd.DataFrame, table_name: str):

        if df is None or df.empty:
            logger.error(f"Cannot insert into '{table_name}': DataFrame is empty or None")
            raise ValueError("DataFrame is empty or None")

        try:
            df.to_sql(
                table_name,
                self.engine,
                if_exists="append",
                index=False,
                method="multi"
            )
            logger.info(f"Inserted {len(df)} rows into '{table_name}' successfully.")
        except Exception as e:
            logger.error(f"Failed to insert rows into '{table_name}': {e}")
            raise
    
    def update_rows(self, table_name: str, set_clause: str, condition: str):

        sql = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE {condition}
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info(f"Updated rows in '{table_name}' where {condition} successfully.")
        except Exception as e:
            logger.error(f"Failed to update rows in '{table_name}': {e}")
            raise