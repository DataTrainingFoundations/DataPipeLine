import os
import pandas as pd
from dotenv import load_dotenv
from src.transform.validation import Validation

load_dotenv()
team_cols = os.getenv('TEAM_COLS').split('|')
fact_cols = os.getenv('FACT_COLS').split('|')
schedule_home = os.getenv("SCHEDULE_HOME").split('|')
schedule_away = os.getenv("SCHEDULE_AWAY").split('|')
game_cols = os.getenv("GAME_COLS").split('|')

def team_table(df):
    t_table = df.rename(columns = {
        'team_abbr': 'team_id',
        'team_id': 'ignore'
    })

    valid, rejected = Validation.valid_columns(t_table, team_cols)

    return valid

def season_table(df):
    s_table = (df[['season']].drop_duplicates())
    s_table["num_games"] = s_table["season"].apply(lambda x: 16 if x < 2021 else 17)

    s_table_rename = s_table.rename(columns = {'season': 'season_id'})
    print(s_table_rename)
    return s_table_rename

def game_table(df):
    g_table = df.rename(columns = {
        'season': 'season_id'
    })
    valid, rejected = Validation.valid_columns(g_table, game_cols)
    valid['game_id'] = valid['season_id'].astype(str) + '_' + valid['week'].astype(str) + '_' + valid['home_team']


    return valid

def facts_table(stats_df, schedule_df):
    if stats_df is None or schedule_df is None:
        raise ValueError("stats_df and schedule_df cannot be None")

    # -----------------------------
    # Build team_games from schedule
    # -----------------------------
    home_games = schedule_df[schedule_home].copy()
    home_games['team_id'] = home_games['home_team']
    home_games['points_scored'] = home_games['home_score']
    home_games['points_allowed'] = home_games['away_score']

    away_games = schedule_df[schedule_away].copy()
    away_games['team_id'] = away_games['away_team']
    away_games['points_scored'] = away_games['away_score']
    away_games['points_allowed'] = away_games['home_score']

    team_games = pd.concat([home_games, away_games], ignore_index=True)

    team_games = team_games.rename(columns={
        'season': 'season_id'
    })

    # -----------------------------
    # Prepare team_stats
    # -----------------------------
    team_stats = stats_df.rename(columns={
        'season': 'season_id',
        'team': 'team_id'
    })

    f_table = team_stats.merge(
        team_games,
        on=['season_id','week','team_id'],
        how='left'
    )

    f_table['game_id'] = (
        f_table['season_id'].astype(str) + "_" +
        f_table['week'].astype(str) + "_" +
        f_table['team_id'].fillna('')
    )

    f_table['result'] = f_table.apply(
        lambda row: 'W' if row['points_scored'] > row['points_allowed']
        else 'L' if row['points_scored'] < row['points_allowed']
        else 'T',
        axis=1
    )

    print(f_table)

    final_table = f_table.rename(columns={
        'attempts': 'pass_attempts',
        'carries': 'rush_attempts',
        'passing_yards': 'pass_yards',
        'rushing_yards': 'rush_yards',
        'passing_tds': 'pass_tds',
        'rushing_tds': 'rush_tds'
    })

    valid, rejected = Validation.valid_columns(final_table, fact_cols)

    return valid
