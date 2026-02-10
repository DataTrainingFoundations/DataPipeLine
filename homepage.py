import streamlit as st
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer
from dotenv import load_dotenv
import os
import json

load_dotenv()


# To run need this line in your .env file

# USED_COLS=posteam|play_type|yards_gained|rush_attempt|pass_attempt|touchdown|pass_touchdown|rush_touchdown

# Simple streamlit homepage to look at dataframes

# In terminal call 'streamlit run homepage.py'

# TO DO:
#     ADD charts
#     Make load data button actually work
#     Transform datframes
#     Toggle for single season data or multi-year data


st.set_page_config(layout="wide")
col1, col2 = st.columns([1,3], border = True)

if 'loaded_dfs' not in st.session_state:
    st.session_state['loaded_dfs'] = []

@st.cache_data
def extract_data(fixed_cols, years):
    return DataExtractor.extract_multiple_files_by_cols(fixed_cols, *years)

@st.cache_data
def clean_data(dfs):
    cleaned_dfs = []
    for df in dfs:
        valid, rejected = DataTransformer.validate(df)
        clean = DataTransformer.clean(valid)
        cleaned_dfs.append(DataTransformer.team_stats(clean))
    
    return cleaned_dfs

def load_data():
    pass

with col1:

    cols_string = os.getenv('USED_COLS')
    cols = cols_string.split('|')

    st.title('RUN vs PASS', text_alignment = 'center')
    st.subheader('An analysis on the NFL', text_alignment = 'center')  
    st.divider(width = 'stretch')

    year_range = list(st.select_slider('Select the range of years you\'d like to analyze', options = range(2009, 2020), value = (2009, 2019)))
    years = list(range(year_range[0], year_range[1] + 1))

    if st.button('Extract data'):
        season_dataframes = extract_data(cols, years)
        st.session_state.loaded_dfs = season_dataframes

    if st.button('Clean data') and st.session_state.loaded_dfs:
        team_stats = clean_data(st.session_state.loaded_dfs)
        st.session_state.loaded_dfs = team_stats

    if st.button('Load data to database') and st.session_state.loaded_dfs:
        pass

with col2:

    if len(years) > 1:
        year_data = st.checkbox('Data by year')
    else:
        year_data = True

    try:
        if st.session_state.loaded_dfs:

            if year_data:
                option = st.selectbox('What dataframe would you like to view?', tuple(years))
                tab1, tab2 = st.tabs(['Charts', 'Dataframe'])
                season_map = {year:df for year,df in zip(years, st.session_state.loaded_dfs)}
                tab2.dataframe(data = season_map[option], width = 'content')
            else:
                st.subheader(f"Stats from {years[0]}-{years[len(years) - 1]}", divider = 'gray')
                #display data over seasons
                pass
    except Exception as e:
        print(f"No dataframe extracted: {e}")



