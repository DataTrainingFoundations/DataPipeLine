import pandas as pd
import requests
import csv
import codecs
import time
from contextlib import closing
from itertools import islice

"""
BASIC EXTRACTOR SO OTHER PARTS CAN BE WORKED ON
FUNCTIONS WILL BE UPDATED WITH STREAMING TO RUN FASTER
OTHER FUNCTIONS WILL BE ADDED AS WELL
"""

class DataExtractor:

    def __init__(self):
        pass

    # Function to extract data from a CSV file, just pass in url
    def extract_from_csv(self, url):
        try:
            # Returns data as a data frame object (requires pandas)
            data = pd.read_csv(url, dtype = str) 
            return data
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None
        
    # Function to extract data from a JSON file, just pass in url
    def extract_from_json(self, url):
        try:
            # Returns data as a data frame object (requires pandas)
            data = pd.read_json(url) 
            return data
        except Exception as e:
            print(f"Error reading JSON file: {e}")
            return None
        
    # Function to extract data from csv given the year/s
    # How to call: DataExtractor.extract_from_csv_by_year(2009, 2010, 2011, ... , 2019)
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
    
    # Function that streams data and returns dictionary of strings
    def stream_data(self, file_path):

        data = {}
        try:
            with closing(requests.get(file_path, stream = True)) as r:
                reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=',')

                line = 0
                for row in reader:
                    if line == 0:
                        line = 1
                        index_map = {key : index for index, key in enumerate(row)}
                        for i in range(len(row)):
                            data.setdefault(row[i], [])
                    else:
                        for k, v in data.items():
                            v.append(row[index_map[k]])
            
            return pd.DataFrame(data)
        except Exception as e:
            print(f"Error streaming file: {e}")
            return None
    
    # Function to stream data by year and returns a list of dictionaries
    def stream_data_by_year(self, *args):

        data_packs = {}
        data = {}

        for year in args:
            base_url = f"https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_{int(year)}.csv"
            data_packs.setdefault(year, {})
            try:
                with closing(requests.get(base_url, stream = True)) as r:
                    reader = csv.reader(codecs.iterdecode(r.iter_lines(), 'utf-8'), delimiter=',')

                    line = 0
                    for row in reader:
                        if line == 0:
                            line = 1
                            index_map = {key : index for index, key in enumerate(row)}
                            for i in range(len(row)):
                                data.setdefault(row[i], [])
                        else:
                            for k, v in data.items():
                                v.append(row[index_map[k]])
            
                data_packs[year].update(data)
            except Exception as e:
                print(f"Error streaming file: {e}")
                return None
            
        return data_packs
