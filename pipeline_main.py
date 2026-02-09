#imports
import pandas as pd
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer

#extract

file_path = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
test_data = DataExtractor.extract_data(file_path)

#transform

valid, rejected = DataTransformer.validate(test_data)
print(rejected.head())
cleaned = DataTransformer.clean(valid)

team_data = DataTransformer.team_stats(cleaned)

#print(team_data)

print(f"2009 total pass attempts: {sum(team_data['pass_attempts'])}")
print(f"2009 total rush attempts: {sum(team_data['rush_attempts'])}")
#load




