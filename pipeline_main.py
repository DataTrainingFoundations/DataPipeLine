#imports
"""Data Pipeline Module handles and exceutes Extraction, Transformation, and Load processes"""
from src.extract.extract_module import DataExtractor
from src.transform.transform_module_team import DataTransformerTeam
from src.load.load_module import DataLoader

#instantiate pipelines

load = DataLoader()

#extract

FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
test_data = DataExtractor.extract_data(FILE_PATH)

#transform

df_valid, df_rejected = DataTransformerTeam.validate(test_data)
df_valid_cleaned = DataTransformerTeam.clean(df_valid)
df_team_data = DataTransformerTeam.team_stats(df_valid_cleaned)

#print(team_data)

print(f"2009 total pass attempts: {sum(df_team_data['pass_attempts'])}")
print(f"2009 total rush attempts: {sum(df_team_data['rush_attempts'])}")
print(df_team_data.info())

#adding rejected data to database
print("Fully rejected team data", df_rejected.head())
rejected_splits = DataTransformerTeam.split_df_rejected(df_rejected)

#load

for i, split_df in enumerate(rejected_splits, start=1):
    table_name = f"Rejected_Team_{i}"
    print(f"Rejected table {i}: {split_df.shape}")
    load.create_(split_df, table_name) 
    load.insert_(split_df, table_name)
