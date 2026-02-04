#imports
import pandas as pd
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer

#extract
extractor = DataExtractor()

file_path = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
test_data = extractor.extract_data(file_path)

#transform
transformer = DataTransformer()

valid, rejected = transformer.validate(test_data)

cleaned = transformer.clean(valid)

#load




