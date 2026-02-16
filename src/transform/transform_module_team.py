import pandas as pd
import numpy as np
""" FROM EXCEL
Attribute: col letter
posteam: E
play_type: Z
yards_gained: AA
rush_attempt: EM
pass_attempt: EN
touchdown: EP
pass_touchdown: EQ
rush_touchdown: ER
"""
class DataTransformerTeam:
    """Module to handle dataframe transformation"""
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
    def clean(df: pd.DataFrame):
        """Cleans the code by dropping any NA's and converting the attributes to int
        Returns a new DataFrame with these specific columns"""
        new_df = df[['team', 'passing_yards', 'rushing_yards', 'attempts', 'carries', 'passing_tds', 'rushing_tds']]
        return new_df
    @staticmethod
    def team_stats(season_data):
        """Gets the team stats for every NFL team"""
        stats = (season_data.groupby(['team'], as_index = False)
        .agg(
            pass_yards = ('passing_yards', 'sum'),
            rush_yards = ('rushing_yards', 'sum'),
            pass_attempts = ('attempts', 'sum'),
            rush_attempts = ('carries', 'sum'),
            pass_touchdowns = ('passing_tds', 'sum'),
            rush_touchdowns = ('rushing_tds', 'sum')
            )
        )

        return stats
