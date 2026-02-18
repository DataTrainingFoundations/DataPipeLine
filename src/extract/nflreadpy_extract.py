import os
import nflreadpy as nfl
import pandas as pd

def get_pbp(year):
    pbp = nfl.load_pbp(year)
    df_pbp = pbp.to_pandas()
    return df_pbp

def get_team_stats(year):
    stats = nfl.load_team_stats(year)
    return stats.to_pandas()

def get_reg_team_stats(year):
    team_stats = nfl.load_team_stats(year)
    reg_team = team_stats.filter(team_stats['season_type'] == 'REG')
    return reg_team.to_pandas()

def get_post_team_stats(year):
    team_stats = nfl.load_team_stats(year)
    reg_team = team_stats.filter(team_stats['season_type'] == 'POST')
    return reg_team.to_pandas()

def get_schedule(year):
    team_schedule = nfl.load_schedules(year)
    reg_team_schedule = team_schedule.filter(team_schedule['game_type'] == 'REG')
    return reg_team_schedule.to_pandas()

def save_team_stats(*args):
    dfs = []
    for year in args:
        ts = nfl.load_team_stats(year)
        reg_ts = ts.filter(ts['season_type'] == 'REG')
        dfs.append(reg_ts.to_pandas())
    return pd.concat(dfs, ignore_index=True)