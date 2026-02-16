import numpy as np
import pytest
import pandas as pd
from src.extract.extract_module import DataExtractor
# from src.load.load_module import DataLoader
from src.transform.transform_module_players import DataTransformerPlayers
from src.transform.transform_module_team import DataTransformerTeam

"""Testing the pipelines"""

extract = DataExtractor()
transform_team = DataTransformerTeam()
transform_players = DataTransformerPlayers()
# load = DataLoader()

def test_extract_team():
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    df = extract.extract_data(FILE_PATH)
    assert not df.empty
    

def test_validate_rows_team():
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff", np.nan, "punt", "run", "run", "pass"],
        "posteam": ["JAC", "ATL", "BOS", "DAL", "DEN", "BOS", "CLE", "PHI"]
    })
    valid_rows, reject_rows = transform_team.validate(df)
    assert len(valid_rows) == 5
    assert len(reject_rows) == 3
    assert valid_rows["posteam"].tolist() == ["JAX", "ATL", "BOS", "CLE", "PHI"]
    assert reject_rows["posteam"].tolist() == ["BOS", "DAL", "DEN"]

def test_clean_rows_team():
    df = pd.DataFrame({
        "play_id": ["1","2","3","4"],
        "game_id": ["1","1","1","1"],
        "posteam": ["DEN", "DEN", "DEN", "DEN"],
        "play_type": ["pass", "run", "pass", "run"],
        "yards_gained": ["23", np.nan, "12", np.nan],
        "rush_attempt": ["0", "1", "0", "1"],
        "pass_attempt": ["1", "0", "1", "0"],
        "touchdown": ["0", "0", "1", "1"],
        "pass_touchdown": ["0", "0", "1", "0"],
        "rush_touchdown" : ["0", "0", "0", "1"]
    })
    cleaned = transform_team.clean(df)
    print(cleaned)
    assert cleaned["yards_gained"].isna().sum() == 0
    assert cleaned["yards_gained"].dtype == "int64"
    assert cleaned["rush_attempt"].dtype == "int64"
    assert cleaned["pass_attempt"].dtype == "int64"
    assert cleaned["touchdown"].dtype == "int64"
    assert cleaned["pass_touchdown"].dtype == "int64"
    assert cleaned["rush_touchdown"].dtype == "int64"
    required_columns = ['posteam', 'play_type', 'yards_gained', 'rush_attempt', 'pass_attempt', 'touchdown', 'pass_touchdown', 'rush_touchdown']
    assert list(cleaned.columns) == required_columns
    
def test_team_stats():

    pass

def test_load_team():
    pass

def test_extract_players():
    pass

def test_transform_players():
    pass

def test_load_players():
    pass


