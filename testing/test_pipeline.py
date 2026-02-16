import numpy as np
import pandas as pd
from src.extract.extract_module import DataExtractor
# from src.load.load_module import DataLoader
from src.transform.transform_module import DataTransformer

"""Testing the pipelines"""

extract = DataExtractor()
transform = DataTransformer()
# load = DataLoader()

def test_extract():
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    df = extract.extract_data(FILE_PATH)
    assert not df.empty
    

def test_validate_rows_team():
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff", np.nan, "punt", "run", "run", "pass"],
        "posteam": ["JAC", "ATL", "BOS", "DAL", "DEN", "BOS", "CLE", "PHI"]
    })
    valid_rows, reject_rows = transform.validate(df)
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
    cleaned = transform.clean(df)
    assert cleaned["yards_gained"].isna().sum() == 0
    assert cleaned["yards_gained"].dtype == "int64"
    assert cleaned["rush_attempt"].dtype == "int64"
    assert cleaned["pass_attempt"].dtype == "int64"
    assert cleaned["touchdown"].dtype == "int64"
    assert cleaned["pass_touchdown"].dtype == "int64"
    assert cleaned["rush_touchdown"].dtype == "int64"
    required_columns = ['posteam', 
                        'play_type', 
                        'yards_gained', 
                        'rush_attempt', 
                        'pass_attempt', 
                        'touchdown', 
                        'pass_touchdown', 
                        'rush_touchdown'
    ]
    assert list(cleaned.columns) == required_columns

def test_split_df_rejected_basic():
    df = pd.DataFrame({
        "posteam": ["ATL", "BOS"],
        "col1": [1, 2],
        "col2": [3, 4],
        "col3": [5, 6],
        "col4": [7, 8]
    })
    splits = transform.split_df_rejected(df, max_cols=3)
    assert len(splits) == 2
    for split in splits:
        assert 'posteam' in split.columns
    for split in splits:
        assert split.shape[1] <= 3


def test_team_stats():
    df = pd.DataFrame({
        "posteam": ["ATL", "ATL", "BOS", "BOS"],
        "yards_gained": [10, 5, 7, 3],
        "pass_attempt": [1, 0, 1, 0],
        "rush_attempt": [0, 1, 0, 1],
        "pass_touchdown": [1, 0, 0, 0],
        "rush_touchdown": [0, 0, 0, 1]
    })
    result = transform.team_stats(df)
    expected_cols = [
        "posteam",
        "pass_yards",
        "rush_yards",
        "pass_attempts",
        "rush_attempts",
        "pass_touchdowns",
        "rush_touchdowns"
    ]
    assert list(result.columns) == expected_cols
    assert set(result['posteam']) == {"ATL", "BOS"}
    team_atl = result[result['posteam'] == "ATL"].iloc[0]
    assert team_atl['pass_yards'] == 10
    assert team_atl['rush_yards'] == 5
    assert team_atl['pass_attempts'] == 1
    assert team_atl['rush_attempts'] == 1
    assert team_atl['pass_touchdowns'] == 1
    assert team_atl['rush_touchdowns'] == 0
    team_bos = result[result['posteam'] == "BOS"].iloc[0]
    assert team_bos['pass_yards'] == 7
    assert team_bos['rush_yards'] == 3
    assert team_bos['pass_attempts'] == 1
    assert team_bos['rush_attempts'] == 1
    assert team_bos['pass_touchdowns'] == 0
    assert team_bos['rush_touchdowns'] == 1
    

def test_load():
    pass



