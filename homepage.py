import streamlit as st
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformer
from dotenv import load_dotenv
import os
import json

load_dotenv()

'''
To run need this line in your .env file

USED_COLS=posteam|play_type|yards_gained|rush_attempt|pass_attempt|touchdown|pass_touchdown|rush_touchdown

Simple streamlit homepage to look at dataframes

In terminal call 'streamlit run homepage.py'

TO DO:
    ADD charts
    Make load data button actually work
    Transform datframes
    Toggle for single season data or multi-year data
'''

st.set_page_config(layout="wide")
col1, col2 = st.columns([1,3])

if 'loaded_dfs' not in st.session_state:
    st.session_state['loaded_dfs'] = []

@st.cache_data
def extract_data(fixed_cols, years):
    return DataExtractor.extract_multiple_files_by_cols(fixed_cols, *years)

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

    if st.button('Load data to database') and st.session_state.loaded_dfs:
        pass

with col2:

    try:
        if st.session_state.loaded_dfs:
            option = st.selectbox('What dataframe would you like to view?', tuple(years))
            season_map = {year:df for year,df in zip(years, st.session_state.loaded_dfs)}
            st.dataframe(data = season_map[option], use_container_width = True)

    except Exception as e:
        print(f"No dataframe extracted: {e}")



