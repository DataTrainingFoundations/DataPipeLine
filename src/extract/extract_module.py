import os
import logging
import pandas as pd
from functools import partial
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(filename = "records.log", level = logging.DEBUG, format = "%(asctime)s - %(levelname)s: %(message)s", filemode = 'a')

class DataExtractor:

    def __init__(self):
        pass

    # Function to extract data from csv or json
    def extract_data(file_path):
        if '.csv' in file_path:
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_csv(file_path, dtype = str) 
                return data
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                return None
            finally:
                logging.debug(f"{file_path} finished extracting")
        elif '.json' in file_path:
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_json(file_path, dtype = str) 
                return data
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                return None
            finally:
                logging.debug(f"{file_path} finished extracting")
        else:
            print('File not in json or csv format')
            return None
        
    # Function to extract certain columns of data
    def extract_from_csv_by_cols(file_path, columns):
        try:
            # Returns data as a data frame object (requires pandas)
            data = pd.read_csv(file_path, usecols = columns, dtype = str) 
            return data
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None
        finally:
                logging.debug(f"{file_path} finished extracting")
        
    # Function to read multiple files
    def extract_multiple_files(*args):

        files = [f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv" for year in args]

        with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
            data_frames = list(pool.map(DataExtractor.extract_data, files))
        
        return data_frames
    

    def extract_multiple_files_by_cols(fixed_columns, *args):

        partialfn = partial(DataExtractor.extract_from_csv_by_cols, columns = fixed_columns)

        files = [f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv" for year in args]

        with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
            data_frames = list(pool.map(partialfn, files))
        
        return data_frames


