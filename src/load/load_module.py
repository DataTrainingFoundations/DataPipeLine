import pandas as pd
from sqlalchemy import text
from src.db.engine import get_engine


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

        with get_engine().begin() as conn:
            conn.execute(text(sql))


    def insert_rows(self, df: pd.DataFrame, table_name: str):

        if df is None or df.empty:
            raise ValueError("DataFrame is empty or None")

        df.to_sql(
            table_name,
            get_engine(),
            if_exists="append",
            index=False,
            method="multi"
        )
    
    def update_rows(self, table_name: str, set_clause: str, condition: str):

        sql = f"""
        UPDATE {table_name}
        SET {set_clause}
        WHERE {condition}
        """

        with get_engine().begin() as conn:
            conn.execute(text(sql))