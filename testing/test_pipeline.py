import numpy as np
import pandas as pd
from src.extract.extract_module import DataExtractor
from src.extract.nflreadpy_extract import get_pbp, get_team_stats, get_schedule, get_teams
from src.load.load_module import DataLoader
from src.transform.transform_module import DataTransformer
from unittest.mock import MagicMock, patch
from src.transform.validation import validation
from src.transform.fe_module import team_table, season_table, game_table, facts_table
from src.transform.cleaning import cleaning

"""Testing the pipeline"""

def test_extract_csv():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    df = extract.extract_data(FILE_PATH, FILE_PATH)
    assert not df.empty

def test_extract_json():
    extract = DataExtractor()
    csv_file = pd.read_csv("player_stats.csv")
    csv_file.to_json("player_stats.json")
    df = extract.extract_data("player_stats.json", "player_stats.json")
    assert not df.empty

def test_extract_invalid_file():
    extract = DataExtractor()
    FILE_PATH = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.txt"
    df = extract.extract_data(FILE_PATH, FILE_PATH)
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

def test_get_schedule():
    df = get_schedule(2009)
    assert not df.empty

def test_get_teams():
    df = get_teams()
    assert not df.empty

def test_validate_rows():
    validate = validation()
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff", np.nan, "punt", "run", "run", "pass"],
        "posteam": ["JAC", "ATL", "NE", "DAL", "DEN", "NE", "CLE", "PHI"]
    })
    valid_rows = validate.valid_rows(df, checked_column='play_type', row_values=['run', 'pass'])
    assert len(valid_rows) == 5
    assert valid_rows["posteam"].tolist() == ["JAX", "ATL", "NE", "CLE", "PHI"]
    assert list(valid_rows['play_type']) == ['run', 'pass', 'run', 'run', 'pass']

def test_validate_columns():
    validate = validation()
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff"],
        "posteam": ["JAC", "ATL", "NE"],
        "yards_gained": [5, 10, 0]
    })
    wanted_columns = ["posteam", "play_type"]
    valid_cols, rejected_cols = validate.valid_columns(df, wanted_columns)
    assert list(valid_cols.columns) == wanted_columns
    assert list(rejected_cols.columns) == ["yards_gained"]

def test_split_df_rejected():
    validate = validation()
    df = pd.DataFrame({
        "posteam": ["ATL", "NE"],
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
    
def test_clean():
    cleaner = cleaning()
    df = pd.DataFrame({
        "numeric_col": [1.5, np.nan, 3.0],
        "string_col": ["a", np.nan, "c"]
    })
    cleaned = cleaner.clean(df)
    assert cleaned["numeric_col"].isna().sum() == 0
    assert cleaned["numeric_col"].dtype == "int64"
    assert cleaned["string_col"].isna().sum() == 0
    assert cleaned["numeric_col"].tolist() == [1, -1, 3]
    assert cleaned["string_col"].tolist() == ["a", "null", "c"]
    

def test_validate_rows_team():
    transform = DataTransformer()
    df = pd.DataFrame({
        "play_type": ["run", "pass", "kickoff", np.nan, "punt", "run", "run", "pass"],
        "posteam": ["JAC", "ATL", "NE", "DAL", "DEN", "NE", "CLE", "PHI"]
    })
    valid_rows, reject_rows = transform.validate(df)
    assert len(valid_rows) == 5
    assert len(reject_rows) == 3
    assert valid_rows["posteam"].tolist() == ["JAX", "ATL", "NE", "CLE", "PHI"]
    assert reject_rows["posteam"].tolist() == ["NE", "DAL", "DEN"]

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
        "posteam": ["ATL", "NE"],
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
        "posteam": ["ATL", "ATL", "NE", "NE"],
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
    assert set(result['posteam']) == {"ATL", "NE"}
    team_atl = result[result['posteam'] == "ATL"].iloc[0]
    assert team_atl['pass_yards'] == 10
    assert team_atl['rush_yards'] == 5
    assert team_atl['pass_attempts'] == 1
    assert team_atl['rush_attempts'] == 1
    assert team_atl['pass_touchdowns'] == 1
    assert team_atl['rush_touchdowns'] == 0
    team_ne = result[result['posteam'] == "NE"].iloc[0]
    assert team_ne['pass_yards'] == 7
    assert team_ne['rush_yards'] == 3
    assert team_ne['pass_attempts'] == 1
    assert team_ne['rush_attempts'] == 1
    assert team_ne['pass_touchdowns'] == 0
    assert team_ne['rush_touchdowns'] == 1
    
def test_team_table():
    df = pd.DataFrame({
        "team_id": ["1", "2"],
        "team_abbr": ["ATL", "NE"],
        "team_name": ["Atlanta Falcons", "New England Patriots"],
        "team_conf": ["NFC", "AFC"],
        "team_division": ["South", "East"]
    })
    team_df = team_table(df)
    assert list(team_df.columns) == ["team_id", "team_name", "team_conf", "team_division"]

def test_season_table():
    df = pd.DataFrame({
        "season": [2009, 2009, 2010, 2022, 2023]
    })
    season_df = season_table(df)
    assert list(season_df.columns) == ["season_id", "num_games"]
    assert season_df['season_id'].tolist() == [2009, 2010, 2022, 2023]
    assert season_df['num_games'].tolist() == [16, 16, 17, 17]

def test_game_table():
    df = pd.DataFrame({
        "season": [2009, 2009],
        "week": [1, 1],
        "home_team": ["ATL", "NE"],
        "game_type": ["REG", "REG"],
        "location": ["Atlanta, GA", "Foxborough, MA"],
        "stadium": ["Mercedes-Benz Stadium", "Gillette Stadium"]
    })
    game_df = game_table(df)
    assert list(game_df.columns) == ["season_id", "week", "home_team", "game_type", "location", "stadium", "game_id"]
    assert game_df['season_id'].tolist() == [2009, 2009]
    assert game_df['game_id'].tolist() == ["2009_1_ATL", "2009_1_NE"]

def test_facts_table():
    stats_df = pd.DataFrame({
        "season": [2009, 2009],
        "team": ["ATL", "NE"],
        "week": [1, 1],
        "season_type": ["REG", "REG"],
        "attempts": [10, 15],
        "carries": [5, 7],
        "passing_yards": [100, 150],
        "rushing_yards": [50, 70],
        "passing_tds": [1, 2],
        "rushing_tds": [0, 1],
    })
    schedule_df = pd.DataFrame({
        "season": [2009, 2009],
        "week": [1, 1],
        "home_team": ["ATL", "NE"],
        "away_team": ["NE", "ATL"],
        "home_score": [20, 30],
        "away_score": [30, 20],
        "game_type": ["REG", "REG"],
        "location": ["Home", "Home"]
    })
    facts_df = facts_table(stats_df, schedule_df)
    expected_cols = [
        'season_id','team_id', 'game_id', 'week', 'game_type',
        'pass_attempts', 'rush_attempts', 'pass_yards', 'rush_yards',
        'pass_tds', 'rush_tds', 'points_scored', 'points_allowed',
        'result'
    ]
    assert list(facts_df.columns) == expected_cols
    assert facts_df['points_scored'].tolist() == [20, 20, 30, 30]
    assert facts_df['points_allowed'].tolist() == [30, 30, 20, 20]
    assert facts_df['result'].tolist() == ['L', 'L', 'W', 'W']

def test_facts_table_exception():
    stats_df = None
    schedule_df = None
    try:
        facts_table(stats_df, schedule_df)
        assert False, "Expected ValueError for None schedule_df"
    except ValueError as e:
        assert str(e) == "stats_df and schedule_df cannot be None"

def test_map_dtype_to_mysql():
    load = DataLoader()
    assert load.map_dtype_to_mysql(pd.Series([1]).dtype) == "BIGINT"
    assert load.map_dtype_to_mysql(pd.Series([1.5]).dtype) == "DOUBLE"
    assert load.map_dtype_to_mysql(pd.Series([True]).dtype) == "BOOLEAN"
    assert load.map_dtype_to_mysql(pd.Series(["x"]).dtype) == "VARCHAR(50)"
    assert load.map_dtype_to_mysql(pd.Series([pd.Timestamp("2020-01-01")]).dtype) == "TIMESTAMP"

@patch("src.load.load_module.get_engine")
@patch("src.load.load_module.inspect")
def test_create_table(mock_get_inspect, mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_inspect = MagicMock()
    mock_inspect.has_table.return_value = False
    mock_get_inspect.return_value = mock_inspect
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "posteam": ["DEN"],
        "yards": [10]
    })
    load.create_(df, table_name="teams", primary_key="posteam")
    assert mock_inspect.has_table.called
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "CREATE TABLE teams" in sql_called
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
@patch("src.load.load_module.inspect")
def test_create_table_no_primary_key(mock_get_inspect, mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_inspect = MagicMock()
    mock_get_inspect.return_value = mock_inspect
    mock_inspect.has_table.return_value = False
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"]
    })
    load.create_(df, table_name="no_pk_table", primary_key=None)
    assert mock_inspect.has_table.called
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "CREATE TABLE no_pk_table" in sql_called
    assert "id SERIAL PRIMARY KEY" in sql_called

@patch("src.load.load_module.get_engine")
@patch("src.load.load_module.inspect")
def test_create_table_primary_key_not_in_df(mock_get_inspect, mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_inspect = MagicMock()
    mock_get_inspect.return_value = mock_inspect
    mock_inspect.has_table.return_value = False
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"]
    })
    load.create_(df, table_name="custom_pk_table", primary_key="custom_id")
    assert mock_inspect.has_table.called
    assert mock_conn.execute.called
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "CREATE TABLE custom_pk_table" in sql_called
    assert "`custom_id` SERIAL PRIMARY KEY" in sql_called

@patch("src.load.load_module.get_engine")
@patch("src.load.load_module.inspect")
def test_create_table_already_exists(mock_get_inspect, mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_inspect = MagicMock()
    mock_get_inspect.return_value = mock_inspect
    mock_inspect.has_table.return_value = True
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "col1": [1, 2],
        "col2": ["a", "b"]
    })
    load.create_(df, table_name="existing_table")
    assert mock_inspect.has_table.called
    assert not mock_conn.execute.called

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
    load.insert_(df, table_name="teams", primary_key="posteam")
    sql_called = str(mock_conn.execute.call_args[0][0])
    print(sql_called)
    assert "INSERT INTO teams" in sql_called
    assert "(`posteam`, `yards`)" in sql_called
    assert "VALUES (:posteam, :yards)" in sql_called
    assert "ON DUPLICATE KEY UPDATE" in sql_called
    assert "`yards`=VALUES(`yards`)" in sql_called
    assert mock_conn.execute.called

@patch("src.load.load_module.get_engine")
def test_insert_table_no_primary_key(mock_get_engine):
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.begin.return_value.__enter__.return_value = mock_conn
    mock_get_engine.return_value = mock_engine
    load = DataLoader()
    df = pd.DataFrame({
        "posteam": ["DEN"],
        "yards": [10]
    })
    load.insert_(df, table_name="teams", primary_key=None)
    sql_called = str(mock_conn.execute.call_args[0][0])
    assert "INSERT INTO teams" in sql_called
    assert "(`posteam`, `yards`)" in sql_called
    assert "VALUES (:posteam, :yards)" in sql_called
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
        load.insert_(empty_df, table_name="teams", primary_key="posteam")
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
        load.insert_(df, table_name="teams", primary_key="posteam")
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
        