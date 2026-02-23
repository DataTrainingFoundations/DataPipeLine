"""Module to handle data cleaning and formating"""
import logging
import pandas as pd

logger = logging.getLogger(__name__)

class Cleaning:
    """Module to handle dataframe cleaning and null value handling"""

    @staticmethod
    def clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the DataFrame by replacing NaN values depending on column type:
        - Numeric columns (int/float): fill NaN with -1, then convert to int
        - Object/string columns: fill NaN with 'null'
        Returns a new cleaned DataFrame.
        """
        df = df.copy()

        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                # Fill numeric NaNs with -1 and convert to int
                df[col] = df[col].fillna(-1).astype(int)
            else:
                # Fill non-numeric NaNs with 'null'
                df[col] = df[col].fillna("null")
        return df
