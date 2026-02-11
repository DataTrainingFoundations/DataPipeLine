"""Load_Module contains logic for loading dataframes into database"""
import logging
import pandas as pd
from sqlalchemy import text
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
        """creates a table using a datafram, and a specified table name 
        with an option to automatically add an incrementing int ID 
        or a specific value for a primary key"""
        if df is None or df.empty:
            logger.error("Cannot create table '%s': DataFrame is empty or None", table_name)
            raise ValueError("DataFrame is empty or None")

        columns = []
        # If no primary key specified
        if primary_key is None:
            columns.append("id SERIAL PRIMARY KEY")

        # Primary Key passed in and insert rows
        for col, dtype in zip(df.columns, df.dtypes):
            sql_type = self.map_dtype_to_mysql(dtype)

            col_quoted = f"`{col}`"
            if col == primary_key:
                columns.append(f"{col_quoted} {sql_type} PRIMARY KEY")
            else:
                columns.append(f"{col_quoted} {sql_type}")


        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)});"

        try:
            with self.engine.begin() as conn:
                conn.execute(text(sql))
            logger.info("Table '%s' created successfully.", table_name)
        except Exception as e:
            logger.error("Failed to create table '%s': %s", table_name, e)
            raise
    def insert_(self, df: pd.DataFrame, table_name: str, primary_key: str = None):
        """Insert rows into a table. 
        Uses upsert if primary_key is provided, normal insert if not."""
        if df is None or df.empty:
            logger.error("Cannot insert into '%s': DataFrame is empty or None", table_name)
            raise ValueError("DataFrame is empty or None")
        # Convert NaN to None for SQL

        df_to_insert = df.where(pd.notnull(df), None)

        columns = list(df.columns)
        placeholders = ", ".join([f":{c}" for c in columns])
        records = df_to_insert.to_dict(orient="records")

        if primary_key is None:
            # Auto-increment PK case → normal insert
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
        else:
            # User-defined PK → upsert
            update_clause = ", ".join([f"{c}=VALUES({c})" for c in columns if c != primary_key])
            sql = f"""INSERT INTO {table_name} ({', '.join(columns)})
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
