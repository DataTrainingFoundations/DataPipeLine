#imports
import time
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer


#extract

#start example
def main():
    transformer = DataTransformer()
    logging.basicConfig(filename = "records.log", level = logging.DEBUG, format = "%(asctime)s - %(levelname)s: %(message)s", filemode = 'a')

    logging.debug("Running example test")
    # file_path = "https://raw.githubusercontent.com/ryurko/nflscrapR-data/refs/heads/master/play_by_play_data/regular_season/reg_pbp_2009.csv"
    cols = ['game_id', 'play_type', 'yards_gained', 'rush_attempt', 'pass_attempt', 'touchdown', 'pass_touchdown', 'rush_touchdown']
    # data = extractor.extract_from_csv_by_cols(file_path, cols)


    # print(data.head(5))

    dataFrames = DataExtractor.extract_multiple_files_by_cols(cols, *[2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019])
    years = []
    valid_dfs = []

    for df in dataFrames:
        years.append(df['game_id'][0][0:4])
        valid, rejected = transformer.validate(df)
        valid_dfs.append(valid)

    # year = 0
    # for df in valid_dfs:
    #     print(f"{years[year]} Data:")
    #     print(df.tail(5))
    #     year += 1
    
    clean_dfs = [transformer.clean(df) for df in valid_dfs]
    year = 0
    sum_rows = []
    for df in clean_dfs:
        totals = df[['pass_attempt', 'rush_attempt']].sum()
        totals['year'] = years[year]
        sum_rows.append(totals)
        year += 1

    data_set = pd.DataFrame(sum_rows).reset_index(drop=True)
    dfm = data_set.melt('year', var_name= 'play_type', value_name= 'total')
    print(data_set)
    sns.relplot(
     data = dfm, kind = 'line', x = 'year', y = 'total', hue="play_type"
    )

    plt.show()

    

    
    


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

    

if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    main()

#end example