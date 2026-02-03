#imports
import pandas as pd
from src.extract_module import DataExtractor
from src.transform_module import DataTransformer

#extract
extractor = DataExtractor()

test_data = extractor.stream_data("https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv")

#transform
transformer = DataTransformer()

valid, rejected = transformer.validate(test_data)

cleaned = transformer.clean(valid)

#load




