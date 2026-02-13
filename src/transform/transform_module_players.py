import pandas as pd

"""Transforms the extracted data for the players"""

class DataTransformerPlayers:

    @staticmethod
    def validate(df: pd.DataFrame):
        """ Validates the rows to add only QB, WR, and RB positions"""
        valid_rows = []
        rejected_rows = []
        
        for _, row in df.iterrows():
            if row['position'] == "QB" or row['position'] == "WR" or row['position'] == "RB":
                valid_rows.append(row)
            else:
                rejected_rows.append(row)

        return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows).fillna('Null')

    @staticmethod
    def clean(df: pd.DataFrame):
        """Cleans the code by dropping any NA's and converting the attributes to int
        Returns a new DataFrame with these specific columns"""
        df["completions"] = df["completions"].dropna().astype(int)
        df["attempts"] = df["attempts"].dropna().astype(int)
        df["passing_yards"] = df["passing_yards"].dropna().astype(int)
        df["passing_tds"] = df["passing_tds"].dropna().astype(int)
        df["carries"] = df["carries"].dropna().astype(int)
        df["rushing_yards"] = df["rushing_yards"].dropna().astype(int)
        df["rushing_tds"] = df["rushing_tds"].dropna().astype(int)
        df["receptions"] = df["receptions"].dropna().astype(int)
        df["targets"] = df["targets"].dropna().astype(int)
        df["receiving_yards"] = df["receiving_yards"].dropna().astype(int)
        df["receiving_tds"] = df["receiving_tds"].dropna().astype(int)
        new_df = df[['player_name', 'position', 'team', 'completions', 'attempts', 'passing_yards', 'passing_tds', 'carries', 'rushing_yards', 'rushing_tds', 'receptions', 'targets', 'receiving_yards', 'receiving_tds']]
        return new_df
        