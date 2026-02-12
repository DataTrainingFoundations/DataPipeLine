import pandas as pd

class DataTransformerPlayers:

    def validate(self, df: pd.DataFrame):
        """ Validates the rows to add only QB, WR, and RB positions"""
        valid_rows = []
        rejected_rows = []
        
        for _, row in df.iterrows():
            if row['position'] == "QB" or row['position'] == "WR" or row['position'] == "RB":
                valid_rows.append(row)
            else:
                rejected_rows.append(row)

        return pd.DataFrame(valid_rows), pd.DataFrame(rejected_rows).fillna('Null')

    def clean(self, df: pd.DataFrame):
        
        pass