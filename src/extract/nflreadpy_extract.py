import os
import logging
import nflreadpy as nfl
import pandas as pd

logging.basicConfig(filename = "records.log",
                    level = logging.DEBUG,
                    format = "%(asctime)s - %(levelname)s: %(message)s",
                    filemode = 'a')

def get_pbp(year):
    pbp = nfl.load_pbp(year)
    logging.debug("%s finished extracting play-by-play")
    return pbp.to_pandas()

def get_team_stats(year = None):
    if year is None:
        stats = nfl.load_team_stats()
    else:
        stats = nfl.load_team_stats(year)
    
    logging.debug("%s finished extracting {year} team stats")
    return stats.to_pandas()

def get_schedule(year = None):
    if year is None:
        team_schedule = nfl.load_schedules()
    else:
        team_schedule = nfl.load_schedules(year)

    logging.debug("%s finished extracting {year} schedule")
    return team_schedule.to_pandas()

def get_teams():
    teams = nfl.load_teams().to_pandas()
    current_teams = teams.drop_duplicates(subset = 'team_id', keep = 'first')

    logging.debug("%s finished extracting teams")
    return current_teams