    
import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class validation:
    """Module to handle dataframe validation choosing columns and choosing rows given a column"""

    @staticmethod
    def valid_rows(df: pd.DataFrame, checked_column: str, row_values: list):
        """ Validates the rows to add only pass and run play types"""
        logger.info(
            "Validating rows | column=%s | allowed_values=%s | input_rows=%s",
            checked_column,
            row_values,
            len(df)
        )
        valid_df = df[df[checked_column].isin(row_values)].copy()
        # Replace 'JAC' with 'JAX' in posteam if the column exists
        if "posteam" in valid_df.columns:
            valid_df["posteam"] = valid_df["posteam"].replace({"JAC": "JAX"})
        logger.info("Row validation complete | output_rows=%s", len(valid_df))
        return valid_df
    
    @staticmethod
    def valid_columns(df: pd.DataFrame, wanted_columns:list):
        """ Validates the rows to add only pass and run play types"""
        logger.info(
            "Validating columns | requested=%s | available=%s",
            len(wanted_columns),
            len(df.columns)
        )
        valid_columns = [col for col in wanted_columns if col in df.columns]
        logger.info("Total columns chosen: %s", len(valid_columns))
        chosen_df = df[valid_columns].copy()
        remaining_df = df.drop(columns=valid_columns).copy()
        logger.info(
            "Column validation complete | chosen_cols=%s | rejected_cols=%s",
            chosen_df.shape[1],
            remaining_df.shape[1]
        )
        return chosen_df, remaining_df
    
    @staticmethod
    def split_df_rejected(df: pd.DataFrame, max_cols: int = 52, primary_id_col: str ='id'):
        """
        Splits a DataFrame into multiple DataFrames with at most `max_cols`
        columns each. Ensures 'posteam' exists in every split.
        Returns: List[pd.DataFrame]
        """
        df = df.copy()

    # Append sequential primary ID column
        df[primary_id_col] = range(1, len(df) + 1)

        # Prepare chunking
        other_cols = [c for c in df.columns if c != primary_id_col]
        chunk_size = max_cols - 1  # reserve one column for the primary ID
        chunks = [other_cols[i:i + chunk_size] for i in range(0, len(other_cols), chunk_size)]

        split_dfs = []

        for idx, cols in enumerate(chunks, start=1):
            split_df = df[[primary_id_col] + cols].copy()
            split_dfs.append(split_df)

            logger.info(
                "Rejected split %s | rows=%s | cols=%s | has_primary_id=%s",
                idx,
                split_df.shape[0],
                split_df.shape[1],
                primary_id_col in split_df.columns
            )

        logger.info("Total rejected splits created: %s", len(split_dfs))
        return split_dfs
