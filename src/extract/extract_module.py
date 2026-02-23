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
            except ValueError as e:
                print(f"Error reading CSV file: {e}")
                return None
            finally:
                logging.debug("%s finished extracting", filename)
        elif filename.endswith(".json"):
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_json(file_obj) 
                return data
            except ValueError as e:
                print(f"Error reading JSON file: {e}")
                return None
            finally:
                logging.debug("%s finished extracting", filename)
        else:
            print('File not in json or csv format')
            return None
    
