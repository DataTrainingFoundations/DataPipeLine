"""Load_Module contains logic for loading dataframes into database"""
import logging
import pandas as pd
from sqlalchemy import inspect,text
from src.db.engine import get_engine

logger = logging.getLogger(__name__)


class DataLoader:
    """Class: contains mapping, create, upsert, and drop methods 
    for tables in the database"""
    def __init__(self):
        self.engine = get_engine()

    @staticmethod
    def map_dtype_to_mysql(dtype):
        """maps pandas dataframe column types to 
        coresponding MySQL column type names"""
        if "int" in str(dtype):
            return "BIGINT"
        elif "float" in str(dtype):
            return "DOUBLE"
        elif "bool" in str(dtype):
            return "BOOLEAN"
        elif "datetime" in str(dtype):
            return "TIMESTAMP"
        else:
            return "VARCHAR(50)"

    def create_(self, df: pd.DataFrame, table_name: str, primary_key: str = "id"):
        """creates a table using a DataFrame, and a specified table name 
        with an option to automatically add an incrementing int ID 
        or a specific value for a primary key"""

        if df is None or df.empty:
            logger.error("Cannot create table '%s': DataFrame is empty or None", table_name)
            raise ValueError("DataFrame is empty or None")

        # --- Check if table exists ---
        inspector = inspect(self.engine)
        if inspector.has_table(table_name):
            logger.info("Table '%s' already exists. Skipping creation.", table_name)
            return

        columns = []

        # --- Handle primary key ---
        if primary_key is None:
            # Case 1: no PK specified → create 'id'
            columns.append("id SERIAL PRIMARY KEY")
            primary_key = None  # nothing in df gets PK
        elif primary_key not in df.columns:
            # Case 2: PK specified but not in df → create that column as PK
            columns.append(f"`{primary_key}` SERIAL PRIMARY KEY")
            primary_key = None  # don't assign PK to any df column
        # Case 3: PK exists in df → will assign in loop

        # --- Add remaining columns ---
        for col, dtype in zip(df.columns, df.dtypes):
            sql_type = self.map_dtype_to_mysql(dtype)
            col_quoted = f"`{col}`"
            if col == primary_key:
                columns.append(f"{col_quoted} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col_quoted} {sql_type}")

        sql = f"CREATE TABLE {table_name} ({', '.join(columns)});"

        # --- Execute SQL ---
        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("Table '%s' created successfully.", table_name)
        except Exception as e:
            logger.error("Failed to create table '%s': %s", table_name, e)
            raise


    def insert_(self, df: pd.DataFrame, table_name: str, primary_key: str):
        """Insert rows into a table. 
        Uses upsert if primary_key is provided, normal insert if not."""
        if df is None or df.empty:
            logger.error("Cannot insert into '%s': DataFrame is empty or None", table_name)
            raise ValueError("DataFrame is empty or None")
        # Convert NaN to None for SQL

        columns = list(df.columns)
        columns_quoted = [f"`{c}`" for c in columns]
        placeholders = ", ".join([f":{c}" for c in columns])
        records = df.to_dict(orient="records")

        if primary_key is None:
        # Auto-increment PK case → normal insert
            sql = f"INSERT INTO {table_name} ({', '.join(columns_quoted)}) VALUES ({placeholders});"
        else:
        # User-defined PK → upsert with backticks
            update_clause = ", ".join([f"`{c}`=VALUES(`{c}`)" for c in columns if c != primary_key])
            sql = f"""INSERT INTO {table_name} ({', '.join(columns_quoted)})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause};"""

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql), records)
            logger.info("Inserted %d rows into '%s' successfully.", len(df), table_name)
        except Exception as e:
            logger.error("Failed to insert rows into '%s': '%s'", table_name,e)
            raise
        
    def drop_(self, table_names):
        """
        Drops one or more tables.
        
        Parameters:
            table_names (str or list[str]): Table name or list of table names to drop.
        """
        if isinstance(table_names, str):
            table_names = [table_names]  # convert single name to list

        for table_name in table_names:
            try:
                with self.engine.begin() as conn:
                    conn.execute(text(f"DROP TABLE IF EXISTS {table_name};"))
                logger.info("Table '%s' dropped successfully.", table_name)
            except Exception as e:
                logger.error("Failed to drop table '%s': '%s", table_name, e)
                raise
