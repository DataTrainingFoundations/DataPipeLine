import numpy as np
import pandas as pd
from src.extract.extract_module import DataExtractor
from src.extract.nflreadpy_extract import get_pbp, get_team_stats, get_reg_team_stats, get_post_team_stats, get_schedule, save_team_stats
from src.load.load_module import DataLoader
from src.transform.transform_module import DataTransformer
from unittest.mock import MagicMock, patch
from src.transform.validation import validation
from src.transform.fe_module import team_stats, team_records, stat_percentages, league_averages

"""Testing the pipeline"""

def test_extract():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    df = extract.extract_data(FILE_PATH)
    assert not df.empty

def test_extract_invalid_file():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.txt"
    df = extract.extract_data(FILE_PATH)
    assert df is None

def test_extract_csv_by_cols():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    columns = ["posteam","play_type","yards_gained"]
    df = extract.extract_from_csv_by_cols(FILE_PATH, columns)
    assert not df.empty
    assert list(df.columns) == columns

def test_extract_csv_by_cols_invalid_column():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    columns = ["posteam","play_type","invalid_column"]
    df = extract.extract_from_csv_by_cols(FILE_PATH, columns)
    assert df is None

def test_get_pbp():
    df = get_pbp(2009)
    assert not df.empty

def test_get_team_stats():
    df = get_team_stats(2009)
    assert not df.empty

def test_get_reg_team_stats():
    df = get_reg_team_stats(2009)
    assert not df.empty
    assert all(df['season_type'] == 'REG')

def test_get_post_team_stats():
    df = get_post_team_stats(2009)
    assert not df.empty
    assert all(df['season_type'] == 'POST')

def test_get_schedule():
    df = get_schedule(2009)
    assert not df.empty

def test_save_team_stats():
    df = save_team_stats(2009, 2010)
    assert not df.empty
    assert set(df['season'].unique()) == {2009, 2010}

def test_validate_rows():
    validate = validation()
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff", np.nan, "punt", "run", "run", "pass"],
        "posteam": ["JAC", "ATL", "BOS", "DAL", "DEN", "BOS", "CLE", "PHI"]
    })
    valid_rows = validate.valid_rows(df, checked_column='play_type', row_values=['run', 'pass'])
    assert len(valid_rows) == 5
    assert valid_rows["posteam"].tolist() == ["JAX", "ATL", "BOS", "CLE", "PHI"]
    assert list(valid_rows['play_type']) == ['run', 'pass', 'run', 'run', 'pass']

def test_validate_columns():
    validate = validation()
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff"],
        "posteam": ["JAC", "ATL", "BOS"],
        "yards_gained": [5, 10, 0]
    })
    wanted_columns = ["posteam", "play_type"]
    valid_cols, rejected_cols = validate.valid_columns(df, wanted_columns)
    assert list(valid_cols.columns) == wanted_columns
    assert list(rejected_cols.columns) == ["yards_gained"]

def test_split_df_rejected():
    validate = validation()
    df = pd.DataFrame({
        "posteam": ["ATL", "BOS"],
        "col1": [1, 2],
        "col2": [3, 4],
        "col3": [5, 6],
        "col4": [7, 8]
    })
    splits = validate.split_df_rejected(df, max_cols=3, primary_id_col="posteam")
    assert len(splits) == 2
    for split in splits:
        assert 'posteam' in split.columns
    for split in splits:
        assert split.shape[1] <= 3
    

def test_validate_rows_team():
    transform = DataTransformer()
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
    transform = DataTransformer()
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
    transform = DataTransformer()
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
    

def test_transform_team_stats():
    transform = DataTransformer()
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
    
def test_team_stats():
    df = pd.DataFrame({
        "team": ["ATL", "ATL", "BOS", "BOS"],
        "passing_yards": [10, 0, 7, 0],
        "rushing_yards": [0, 5, 0, 6],
        "attempts": [1, 0, 1, 0],
        "carries": [0, 1, 0, 1],
        "passing_tds": [1, 0, 0, 0],
        "rushing_tds": [0, 0, 0, 1]
    })
    result = team_stats(df)
    expected_cols = [
        "team",
        "pass_yards",
        "rush_yards",
        "pass_attempts",
        "rush_attempts",
        "pass_touchdowns",
        "rush_touchdowns"
    ]
    assert list(result.columns) == expected_cols
    assert set(result['team']) == {"ATL", "BOS"}
    team_atl = result[result['team'] == "ATL"].iloc[0]
    assert team_atl['pass_yards'] == 10
    assert team_atl['rush_yards'] == 5
    assert team_atl['pass_attempts'] == 1
    assert team_atl['rush_attempts'] == 1
    assert team_atl['pass_touchdowns'] == 1
    assert team_atl['rush_touchdowns'] == 0
    team_bos = result[result['team'] == "BOS"].iloc[0]
    assert team_bos['pass_yards'] == 7
    assert team_bos['rush_yards'] == 6
    assert team_bos['pass_attempts'] == 1
    assert team_bos['rush_attempts'] == 1
    assert team_bos['pass_touchdowns'] == 0
    assert team_bos['rush_touchdowns'] == 1

def test_team_records():
    df = pd.DataFrame({
        "season": [2009, 2009, 2009, 2009],
        "home_team": ["ATL", "BOS", "ATL", "BOS"],
        "away_team": ["BOS", "ATL", "BOS", "ATL"],
        "home_score": [20, 10, 15, 25],
        "away_score": [10, 20, 25, 15]
    })
    result = team_records(df)
    expected_cols = ["season", "team", "wins", "losses"]
    assert list(result.columns) == expected_cols
    assert set(result['team']) == {"ATL", "BOS"}
    team_atl = result[result['team'] == "ATL"].iloc[0]
    assert team_atl['season'] == 2009
    assert team_atl['wins'] == 2
    assert team_atl['losses'] == 2
    team_bos = result[result['team'] == "BOS"].iloc[0]
    assert team_bos['season'] == 2009
    assert team_bos['wins'] == 2
    assert team_bos['losses'] == 2

def test_stat_percentages():
    df_stats = pd.DataFrame({
        "team": ["ATL", "BOS"],
        "pass_yards": [100, 50],
        "rush_yards": [50, 100],
        "pass_attempts": [10, 5],
        "rush_attempts": [5, 10],
        "pass_touchdowns": [2, 1],
        "rush_touchdowns": [1, 2]
    })
    df_record = pd.DataFrame({
        "season": [2009, 2009],
        "team": ["ATL", "BOS"],
        "wins": [10, 5],
        "losses": [6, 11]
    })
    result = stat_percentages(df_stats, df_record)
    expected_cols = [
        'team', 
        'pass_percent',
        'rush_percent',
        'ypa',
        'ypr',
        'win_percent'
    ]
    assert list(result.columns) == expected_cols
    team_atl = result[result['team'] == "ATL"].iloc[0]
    assert team_atl['pass_percent'] == 10 / (10 + 5)
    assert team_atl['rush_percent'] == 5 / (10 + 5)
    assert team_atl['ypa'] == 100 / 10
    assert team_atl['ypr'] == 50 / 5
    assert team_atl['win_percent'] == 10 / (10 + 6)
    team_bos = result[result['team'] == "BOS"].iloc[0]
    assert team_bos['pass_percent'] == 5 / (5 + 10)
    assert team_bos['rush_percent'] == 10 / (5 + 10)
    assert team_bos['ypa'] == 50 / 5
    assert team_bos['ypr'] == 100 / 10
    assert team_bos['win_percent'] == 5 / (5 + 11)

def test_league_averages():
    df_stats = pd.DataFrame({
        "team": ["ATL", "BOS"],
        "pass_attempts": [10, 5],
        "rush_attempts": [5, 10]
    })
    result = league_averages(df_stats)
    assert result['avg_pass_attempts'] == 7.5
    assert result['avg_rush_attempts'] == 7.5
    total_pass = 10 + 5
    total_rush = 5 + 10
    total_plays = total_pass + total_rush
    assert result['avg_pass_percent'] == total_pass / total_plays
    assert result['avg_rush_percent'] == total_rush / total_plays

def test_map_dtype_to_mysql():
    load = DataLoader()
    assert load.map_dtype_to_mysql(pd.Series([1]).dtype) == "BIGINT"
    assert load.map_dtype_to_mysql(pd.Series([1.5]).dtype) == "DOUBLE"
    assert load.map_dtype_to_mysql(pd.Series([True]).dtype) == "BOOLEAN"
    assert load.map_dtype_to_mysql(pd.Series(["x"]).dtype) == "VARCHAR(50)"
    assert load.map_dtype_to_mysql(pd.Series([pd.Timestamp("2020-01-01")]).dtype) == "TIMESTAMP"

@patch("src.load.load_module.get_engine")
def test_create_table(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "posteam": ["DEN"],
        "yards": [10]
    })
    load.create_(df, table_name="teams", primary_key="posteam")
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "CREATE TABLE IF NOT EXISTS teams" in sql_called
    assert "`posteam` VARCHAR(50) PRIMARY KEY" in sql_called
    assert "`yards` BIGINT" in sql_called
    
@patch("src.load.load_module.get_engine")
def test_create_table_empty_df(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    empty_df = pd.DataFrame()
    try:
        load.create_(empty_df, table_name="empty_table")
        assert False, "Expected ValueError for empty DataFrame"
    except ValueError as e:
        assert str(e) == "DataFrame is empty or None"

@patch("src.load.load_module.get_engine")
def test_create_table_no_primary_key(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"]
    })
    load.create_(df, table_name="no_pk_table", primary_key=None)
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "CREATE TABLE IF NOT EXISTS no_pk_table" in sql_called
    assert "id SERIAL PRIMARY KEY" in sql_called

@patch("src.load.load_module.get_engine")
def test_create_table_exception(mock_get_engine):
    mock_engine = MagicMock()
    mock_engine.begin.side_effect = Exception("DB error")
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"]
    })
    try:
        load.create_(df, table_name="error_table")
        assert False, "Expected Exception for DB error"
    except Exception as e:
        assert str(e) == "DB error"

@patch("src.load.load_module.get_engine")
def test_insert_table(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "posteam": ["DEN"],
        "yards": [10]
    })
    load.insert_(df, table_name="teams")
    assert mock_conn.execute.called

@patch("src.load.load_module.get_engine")
def test_insert_table_empty_df(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    empty_df = pd.DataFrame()
    try:
        load.insert_(empty_df, table_name="teams")
        assert False, "Expected ValueError for empty DataFrame"
    except ValueError as e:
        assert str(e) == "DataFrame is empty or None"

@patch("src.load.load_module.get_engine")
def test_insert_table_exception(mock_get_engine):
    mock_engine = MagicMock()
    mock_engine.begin.side_effect = Exception("DB error on insert")
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "posteam": ["DEN"],
        "yards": [10]
    })
    try:
        load.insert_(df, table_name="teams")
        assert False, "Expected Exception for DB error on insert"
    except Exception as e:
        assert str(e) == "DB error on insert"

@patch("src.load.load_module.get_engine")
def test_drop_table(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    load.drop_("teams")
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "DROP TABLE IF EXISTS teams" in sql_called

@patch("src.load.load_module.get_engine")
def test_drop_table_multiple(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    load.drop_(["teams", "players"])
    assert mock_conn.execute.call_count == 2
    sql_called_1 = str(mock_conn.execute.call_args_list[0][0][0])
    sql_called_2 = str(mock_conn.execute.call_args_list[1][0][0])
    assert "DROP TABLE IF EXISTS teams" in sql_called_1
    assert "DROP TABLE IF EXISTS players" in sql_called_2

@patch("src.load.load_module.get_engine")
def test_drop_table_exception(mock_get_engine):
    mock_engine = MagicMock()
    mock_engine.begin.side_effect = Exception("DB error on drop")
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    try:
        load.drop_("teams")
        assert False, "Expected Exception for DB error on drop"
    except Exception as e:
        assert str(e) == "DB error on drop"
        