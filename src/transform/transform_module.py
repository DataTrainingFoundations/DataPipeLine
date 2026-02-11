import pandas as pd
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
class DataTransformer:

    # Validates the rows to add only pass and run play types
    def validate(df: pd.DataFrame):
        valid_rows = []
        rejected_rows = []
        
        for _, row in df.iterrows():
            if (row["play_type"] == "pass" or row["play_type"] == "run"):
                if (row["posteam"] == "JAC"):
                    row["posteam"] = "JAX"
                valid_rows.append(row)
            else:
                rejected_rows.append(row)
        return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows)
    
    # Cleans the code by dropping any NA's and converting the attributes to int
    # Returns a new DataFrame with these specific columns
    def clean(df: pd.DataFrame):
        df["yards_gained"] = df["yards_gained"].dropna().astype(int)
        df["rush_attempt"] = df["rush_attempt"].astype(int)
        df["pass_attempt"] = df["pass_attempt"].astype(int)
        df["touchdown"] = df["touchdown"].astype(int)
        df["pass_touchdown"] = df["pass_touchdown"].astype(int)
        df["rush_touchdown"] = df["rush_touchdown"].astype(int)
        new_df = df[['posteam', 'play_type', 'yards_gained', 'rush_attempt', 'pass_attempt', 'touchdown', 'pass_touchdown', 'rush_touchdown']]
        return new_df
    
    # Gets the team stats for every NFL team
    def team_stats(season_data):
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
  





