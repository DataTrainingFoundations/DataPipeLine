#imports
"""Data Pipeline Module handles and exceutes Extraction, Transformation, and Load processes"""
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer
from src.transform.validation import validation
from src.transform.cleaning import cleaning
from src.load.load_module import DataLoader

#instantiate pipelines

load = DataLoader()

#extract

FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
test_data = DataExtractor.extract_data(FILE_PATH)
columns = ["posteam","play_type","yards_gained","rush_attempt","pass_attempt", "touchdown","pass_touchdown", "rush_touchdown"]
rows = ['pass', 'run']

#oldtransform

df_valid, df_rejected = DataTransformer.validate(test_data)
df_valid_cleaned = DataTransformer.clean(df_valid)
df_team_data = DataTransformer.team_stats(df_valid_cleaned)

#print(team_data)

print(f"2009 total pass attempts: {sum(df_team_data['pass_attempts'])}")
print(f"2009 total rush attempts: {sum(df_team_data['rush_attempts'])}")
print(df_team_data.info())

#new transform
print("new transform")

validated_columns,rejected_team_stats = validation.valid_columns(df=test_data, wanted_columns=columns)
validated_team_stats = validation.valid_rows(df=validated_columns, checked_column='play_type',row_values=rows)
validated_and_cleaned_teams = cleaning.clean(df = validated_team_stats)


print(validated_and_cleaned_teams.head())

#adding rejected data to database
print("Fully rejected team data", rejected_team_stats.head())
print("fully rejected team stats info", rejected_team_stats.info)

#split rejected table
validated_rejection_table = cleaning.clean(df=rejected_team_stats)
rejected_team_stats_splits = validation.split_df_rejected(df=rejected_team_stats)
print("split 2" ,rejected_team_stats_splits[1])

'''
for i, split_df in enumerate(rejected_splits, start=1):
    table_name = f"Rejected_Team_{i}"
    print(f"Rejected table {i}: {split_df.shape}")
    load.create_(split_df, table_name) 
    load.insert_(split_df, table_name)
'''