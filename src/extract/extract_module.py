import os
import logging
import pandas as pd
from functools import partial
from concurrent.futures import ProcessPoolExecutor

logging.basicConfig(filename = "records.log",
                    level = logging.DEBUG,
                    format = "%(asctime)s - %(levelname)s: %(message)s",
                    filemode = 'a')

class DataExtractor:
    """Module to handle extracting sources"""

    def __init__(self):
        pass

    @staticmethod
    def extract_data(file_obj, filename):
        """Function to extract data from csv or json"""
        if filename.endswith(".csv"):
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_csv(file_obj) 
                return data
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                return None
            finally:
                logging.debug("%s finished extracting", filename)
        elif filename.endswith(".json"):
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_json(file_obj) 
                return data
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                return None
            finally:
                logging.debug("%s finished extracting", filename)
        else:
            print('File not in json or csv format')
            return None
    @staticmethod
    def extract_from_csv_by_cols(file_path, columns):
        """Function to extract certain columns of data"""
        try:
            # Returns data as a data frame object (requires pandas)
            data = pd.read_csv(file_path, usecols = columns, dtype = str) 
            return data
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None
        finally:
            logging.debug("%s finished extracting", file_path)
    # @staticmethod
    # def extract_multiple_files(*args):
    #     """Function to read multiple files"""
    #     files = [f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv" for year in args]

    #     with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
    #         data_frames = list(pool.map(DataExtractor.extract_data, files))

    #     return data_frames

    # @staticmethod
    # def extract_multiple_files_by_cols(fixed_columns, *args):
    #     """Function to extact multiple files, by column name"""
    #     partialfn = partial(DataExtractor.extract_from_csv_by_cols, columns = fixed_columns)

    #     files = [f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv" for year in args]

    #     with ProcessPoolExecutor(max_workers = os.cpu_count()) as pool:
    #         data_frames = list(pool.map(partialfn, files))
    #     return data_frames
