import numpy as np
import pandas as pd
import logging

logger = logging.getLogger(__name__)
class DataTransformerTeam:
    """Module to handle dataframe transformation"""
    """ FROM EXCEL Attribute: 
        col letter
        posteam: E
        play_type: Z
        yards_gained: AA
        rush_attempt: EM
        pass_attempt: EN
        touchdown: EP
        pass_touchdown: EQ
        rush_touchdown: ER
    """
    @staticmethod
    def validate(df: pd.DataFrame):
        """ Validates the rows to add only pass and run play types"""
        valid_rows = []
        rejected_rows = []
        
        for _, row in df.iterrows():
            if (row["play_type"] == "pass" or row["play_type"] == "run"):
                if (row["posteam"] == "JAC"):
                    row["posteam"] = "JAX"
                valid_rows.append(row)
            else:
                rejected_rows.append(row)
                
        return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows).fillna('Null')
    
    @staticmethod
    def split_df_rejected(df: pd.DataFrame, max_cols: int = 52):
        """
        Splits a DataFrame into multiple DataFrames with at most `max_cols`
        columns each. Ensures 'posteam' exists in every split.
        Returns: List[pd.DataFrame]
        """
        if 'posteam' not in df.columns:
            raise ValueError("posteam column is required for splitting")

        # Remove posteam temporarily so we can chunk the rest
        other_cols = [c for c in df.columns if c != 'posteam']
        chunk_size = max_cols - 1  # reserve space for posteam
        chunks = [other_cols[i:i + chunk_size] for i in range(0, len(other_cols), chunk_size)]

        split_dfs = []

        for idx, cols in enumerate(chunks, start=1):
            split_df = df[['posteam'] + cols].copy()
            split_dfs.append(split_df)

            logger.info(
                "Rejected split %s | rows=%s | cols=%s | has_posteam=%s",
                idx,
                split_df.shape[0],
                split_df.shape[1],
                'posteam' in split_df.columns
            )

        logger.info("Total rejected splits created: %s", len(split_dfs))
        return split_dfs



    
    @staticmethod
    def clean(df: pd.DataFrame):
        """Cleans the code by dropping any NA's and converting the attributes to int
        Returns a new DataFrame with these specific columns"""
        df["yards_gained"] = df["yards_gained"].dropna().astype(int)
        df["rush_attempt"] = df["rush_attempt"].astype(int)
        df["pass_attempt"] = df["pass_attempt"].astype(int)
        df["touchdown"] = df["touchdown"].astype(int)
        df["pass_touchdown"] = df["pass_touchdown"].astype(int)
        df["rush_touchdown"] = df["rush_touchdown"].astype(int)
        new_df = df[['posteam', 'play_type', 'yards_gained', 'rush_attempt', 'pass_attempt', 'touchdown', 'pass_touchdown', 'rush_touchdown']]
        return new_df
    @staticmethod
    def team_stats(season_data):
        """Gets the team stats for every NFL team"""
        season_data['pass_yards'] = season_data['yards_gained'] * season_data['pass_attempt']
        season_data['rush_yards'] = season_data['yards_gained'] * season_data['rush_attempt']

        stats = (season_data.groupby('posteam', as_index = False)
                .agg(
                    pass_yards = ('pass_yards', 'sum'),
                    rush_yards = ('rush_yards', 'sum'),
                    pass_attempts = ('pass_attempt', 'sum'),
                    rush_attempts = ('rush_attempt', 'sum'),
                    pass_touchdowns = ('pass_touchdown', 'sum'),
                    rush_touchdowns = ('rush_touchdown', 'sum'),
                    )
                )

        return stats
