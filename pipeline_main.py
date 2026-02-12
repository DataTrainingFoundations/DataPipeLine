#imports
"""Data Pipeline Module handles and exceutes Extraction, Transformation, and Load processes"""
from src.extract.extract_module import DataExtractor
from src.transform.transform_module_team import DataTransformer
from src.load.load_module import DataLoader

#instantiate pipelines
extract = DataExtractor()
transform = DataTransformer()
load = DataLoader()

#extract

FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
test_data = extract.extract_data(FILE_PATH)

#transform

valid, rejected = transform.validate(test_data)
print(rejected.head())
cleaned = transform.clean(valid)

team_data = transform.team_stats(cleaned)

#print(team_data)

print(f"2009 total pass attempts: {sum(team_data['pass_attempts'])}")
print(f"2009 total rush attempts: {sum(team_data['rush_attempts'])}")
print(team_data.info())

#load


TEAM_TABLE = 'Team_Stats'
REJECTED_TABLE = 'Rejected'

load.create_(df=team_data, table_name=TEAM_TABLE, primary_key="team_id")
load.create_(df= rejected,table_name=REJECTED_TABLE)
load.insert_(df=team_data, table_name=TEAM_TABLE,primary_key="posteam")
load.insert_(df= rejected,table_name=REJECTED_TABLE)
