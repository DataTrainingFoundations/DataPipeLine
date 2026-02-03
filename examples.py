#imports
import pandas as pd
from src.extract_module import DataExtractor


#extract

#start example
extractor = DataExtractor()
'''
zero_nine_data = extractor.extract_from_csv("https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv")
print(zero_nine_data['play_type'].head(5))

multi_year_data = extractor.extract_from_csv_by_year(2010, 2011, 2012)
#remember extract_from_csv_by_year returns list of data frames so for loop is needed to go through them
print_year = 2010 #just to make it look nice
for year in multi_year_data:
    print(f"{print_year} data:\n") #just to make it look nice
    print(year['play_type'].head(5))
    print_year += 1
'''
# stream_test = extractor.stream_data_by_year(2009, 2010, 2011)
# print(type(stream_test[2009]))

# for i, item in enumerate(stream_test[2009]['play_type']):
#     if i < 10:
#         print(item)
#     else:
#         break

stream_two = extractor.stream_data("https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv")
print(stream_two['play_type'].head(5))
#end example