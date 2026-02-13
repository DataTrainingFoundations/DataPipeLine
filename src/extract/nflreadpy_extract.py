import os
import nflreadpy as nfl
from concurrent.futures import ProcessPoolExecutor

def get_pbp(year):
    pbp = nfl.load_pbp(year)
    df_pbp = pbp.to_pandas()
    return df_pbp

def get_multiple_pbp(*args):
    with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
            data_frames = list(pool.map(get_pbp, args))
    return data_frames

def get_player_stats(year):
    player_stats = nfl.load_player_stats(year)
    df_player = player_stats.to_pandas()
    return df_player

def get_multiple_player(*args):
    with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
            data_frames = list(pool.map(get_player_stats, args))
    return data_frames

