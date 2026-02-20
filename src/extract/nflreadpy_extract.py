import os
import nflreadpy as nfl
import pandas as pd

def get_pbp(year):
    pbp = nfl.load_pbp(year)
    return pbp.to_pandas()

def get_team_stats(year = None):
    if year is None:
        stats = nfl.load_team_stats()
    else:
        stats = nfl.load_team_stats(year)
    return stats.to_pandas()

def get_schedule(year = None):
    if year is None:
        team_schedule = nfl.load_schedules()
    else:
        team_schedule = nfl.load_schedules(year)
    return team_schedule.to_pandas()

def get_teams():
    teams = nfl.load_teams().to_pandas()
    current_teams = teams.drop_duplicates(subset = 'team_id', keep = 'first')
    return current_teams