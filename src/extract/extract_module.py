import pandas as pd

class DataExtractor:

    def __init__(self):
        pass

    # Function to extract data from csv or json
    def extract_data(self, file_path):
        if '.csv' in file_path:
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_csv(file_path, dtype = str) 
                return data
            except Exception as e:
                print(f"Error reading CSV file: {e}")
                return None
        elif '.json' in file_path:
            try:
                # Returns data as a data frame object (requires pandas)
                data = pd.read_json(file_path, dtype = str) 
                return data
            except Exception as e:
                print(f"Error reading JSON file: {e}")
                return None
        else:
            print('File not in json or csv format')
            return None
        
    # Function to extract data from csv given the year/s
    def extract_from_csv_by_year(self, *args):
        # Returns list of data frames (requires pandas)
        data_frames = []

        for year in args:
            url = f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv"

            try:
                data = pd.read_csv(url, dtype = str)
                data_frames.append(data)
            except Exception as e:
                print(f"Error reading {year} CSV file: {e}")
        
        return data_frames
    
    
    # Function to extract certain columns of data
    def extract_from_csv_by_cols(self, file_path, columns):
        try:
            # Returns data as a data frame object (requires pandas)
            data = pd.read_csv(file_path, usecols = columns, dtype = str) 
            return data
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None


