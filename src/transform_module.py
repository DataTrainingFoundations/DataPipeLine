import pandas as pd
""" FROM EXCEL
Attribute: col letter
play_type: Z
yards_gained: AA
rush_attempt: EM
pass_attempt: EN
touchdown: EP
pass_touchdown: EQ
rush_touchdown: ER
"""
class DataTransformer:

    def validate(self, df: pd.DataFrame):
        valid_rows = []
        rejected_rows = []
        for _, row in df.iterrows():
            if row["play_type"] == "pass" or row["play_type"] == "run":
                valid_rows.append(row)
            else:
                rejected_rows.append(row)
        return pd.DataFrame(valid_rows), rejected_rows
            
    def clean(self, df: pd.DataFrame):
        df["yards_gained"] = df["yards_gained"].astype(int)
        df["rush_attempt"] = df["rush_attempt"].astype(int)
        df["pass_attempt"] = df["pass_attempt"].astype(int)
        df["touchdown"] = df["touchdown"].astype(int)
        df["pass_touchdown"] = df["pass_touchdown"].astype(int)
        df["rush_touchdown"] = df["rush_touchdown"].astype(int)
        new_df = df[['play_type', 'yards_gained', 'rush_attempt', 'pass_attempt', 'touchdown', 'pass_touchdown', 'rush_touchdown']]
        return new_df




