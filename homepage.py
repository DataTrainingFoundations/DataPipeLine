# pylint: disable=import-error
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from src.extract.extract_module import DataExtractor
from src.transform.transform_module import DataTransformerTeam
from src.load.load_module import DataLoader
from dotenv import load_dotenv
import os
import json

load_dotenv()
extract = DataExtractor()
load = DataLoader()


# To run need this line in your .env file

# USED_COLS=posteam|play_type|yards_gained|rush_attempt|pass_attempt|touchdown|pass_touchdown|rush_touchdown

# Simple streamlit homepage to look at dataframes

# In terminal call 'streamlit run homepage.py'

# TO DO:
#     ADD charts
#     Toggle for single season data or multi-year data


st.set_page_config(layout="wide")
col1, col2 = st.columns([1,3], border = True)

if 'loaded_dfs' not in st.session_state:
    st.session_state['loaded_dfs'] = []

if 'cleaned' not in st.session_state:
    st.session_state['cleaned'] = False

if 'sort_column' not in st.session_state:
    st.session_state['sort_column'] = None

if 'option' not in st.session_state:
    st.session_state['option'] = None
if 'current_fig' not in st.session_state:
    st.session_state['current_fig'] = None

@st.cache_data
def extract_data(fixed_cols, years):
    return DataExtractor.extract_multiple_files_by_cols(fixed_cols, *years)

@st.cache_data
def clean_data(dfs):
    cleaned_dfs = []
    for df in dfs:
        valid, rejected = DataTransformerTeam.validate(df)
        clean = DataTransformerTeam.clean(valid)
        cleaned_dfs.append(DataTransformerTeam.team_stats(clean))
    
    return cleaned_dfs

def load_data(dfs, years):
    for i, df in enumerate(dfs):
        table_name = f'{years[i]}_team_stats'
        load.create_(df = df, table_name = table_name, primary_key = 'posteam')
        load.insert_(df = df, table_name= table_name, primary_key='posteam')

def reset_sorted():
    st.session_state.sort_column = None

def build_bar_chart(df, y_cols, title):
    fig, ax = plt.subplots(figsize=(10, 4.5))

    x = range(len(df))
    width = 0.4

    ax.bar(
        [i - width/2 for i in x],
        df[y_cols[0]],
        width,
        label=y_cols[0],
        color='blue'
    )

    ax.bar(
        [i + width/2 for i in x],
        df[y_cols[1]],
        width,
        label=y_cols[1],
        color='orange'
    )

    ax.set_xticks(x)
    ax.set_xticklabels(df['posteam'], rotation=45, ha='right')
    ax.set_title(title)
    ax.legend()

    plt.tight_layout()

    return fig


def show_team_data():
    option = st.session_state['option']
    tab1, tab2 = st.tabs(['Dataframe', 'Charts'])
    season_map = {year:df for year,df in zip(years, st.session_state.loaded_dfs)}
    df = season_map[option]
    tab1.dataframe(data = df, width = 'content')
                
    with tab2:

        tab21, tab22, tab23, tab24 = st.tabs(['Pass v Rush Yards','Pass Attempts vs Rush Attempts', 'Correlation Matrix', 'Scatter Plots'])

        with tab21:
            sort_col1, sort_col2, _ = st.columns([2.5, 2.5, 10], gap = 'xxsmall')
            if sort_col1.button("Sort by Pass Yards"):
                st.session_state.sort_column = 'pass_yards'

            if sort_col2.button("Sort by Rush Yards"):
                st.session_state.sort_column = 'rush_yards'

            if st.session_state.get('sort_column'):
                df = df.sort_values(by=st.session_state.sort_column,ascending=False)

            fig1 = build_bar_chart(
                df,
                ['pass_yards', 'rush_yards'],
                'Pass Yards vs Rush Yards'
            )
            st.session_state.current_fig = fig1
            st.pyplot(st.session_state.current_fig)

        with tab22:
            fig2 = build_bar_chart(
                df,
                ['pass_attempts', 'rush_attempts'],
                'Pass Attempts vs Rush Attempts'
            )
            st.pyplot(fig2)

        with tab23:
            corr = df[['pass_yards', 'pass_touchdowns', 'pass_attempts', 'rush_yards', 'rush_touchdowns', 'rush_attempts']].corr()
            corr_squared = corr ** 2
            fig3, ax3 = plt.subplots(figsize=(5, 4))
            sns.heatmap(
                corr_squared,
                annot=True,
                cmap='coolwarm',
                fmt=".2f",
                ax=ax3,
                annot_kws={'fontsize': 8},
                cbar_kws={"shrink": 0.8},
                square=True
            )
            ax3.tick_params(axis='x', labelsize=8, rotation=45)
            ax3.tick_params(axis='y', labelsize=8)

            ax3.set_title("Correlation Matrix", fontsize=10)
            st.pyplot(fig3)
        
        with tab24:
            pass

def show_year_data():
    season_map = {year:df for year,df in zip(years, st.session_state.loaded_dfs)}
    
    attempts_rows = []

    for year, df in season_map.items():
        attempts_rows.append({
            'season' : year,
            'pass_attempts' : int(df['pass_attempts'].astype('Int64').sum()),
            'rush_attempts' : int(df['rush_attempts'].astype('Int64').sum())
        })

    yearly_attempts = pd.DataFrame(attempts_rows).sort_values('season')

    fig, ax = plt.subplots(figsize=(12, 4.5))
    ax.plot(yearly_attempts['season'], yearly_attempts['pass_attempts'], label='Pass Attempts')
    ax.plot(yearly_attempts['season'], yearly_attempts['rush_attempts'], label='Rush Attempts')

    ax.set_title('NFL Pass vs Rush Attempts Over Time')
    ax.set_xlabel('Season')
    ax.set_ylabel('Attempts')
    ax.legend()
    ax.grid(alpha=0.3)

    st.pyplot(fig)

    yards_rows = []

    for year, df in season_map.items():
        yards_rows.append({
            'season' : year,
            'pass_yards' : int(df['pass_yards'].astype('Int64').sum()),
            'rush_yards' : int(df['rush_yards'].astype('Int64').sum())
        })

    yearly_yards = pd.DataFrame(yards_rows).sort_values('season')

    fig2, ax2 = plt.subplots(figsize=(12, 4.5))
    ax2.plot(yearly_yards['season'], yearly_yards['pass_yards'], label='Pass Yards')
    ax2.plot(yearly_yards['season'], yearly_yards['rush_yards'], label='Rush Yards')

    ax2.set_title('NFL Pass vs Rush Yards Over Time')
    ax2.set_xlabel('Season')
    ax2.set_ylabel('Yards')
    ax2.legend()
    ax2.grid(alpha=0.3)

    st.pyplot(fig2)



with col1:

    cols_string = os.getenv('USED_COLS')
    cols = cols_string.split('|')

    st.title('RUN vs PASS', text_alignment = 'center', anchor=False)
    st.subheader('An analysis on the NFL', text_alignment = 'center', anchor=False)  
    st.divider(width = 'stretch')

    year_range = list(st.select_slider('Select the range of years you\'d like to analyze', options = range(2009, 2020), value = (2009, 2019)))
    years = list(range(year_range[0], year_range[1] + 1))
    if st.session_state.get('option')not in years:
        st.session_state.option = years[0]

    if st.button('Extract data'):
        season_dataframes = extract_data(cols, years)
        st.session_state.loaded_dfs = season_dataframes
        st.session_state.cleaned = False


    if st.button('Clean data') and st.session_state.loaded_dfs:
        team_stats = clean_data(st.session_state.loaded_dfs)
        st.session_state.loaded_dfs = team_stats
        st.session_state.cleaned = True

    if st.button('Load data to database') and st.session_state.loaded_dfs and st.session_state.cleaned:
        load_data(st.session_state.loaded_dfs, years)

with col2:

    if len(years) > 1:
        year_data = st.checkbox('Data by year')
    else:
        year_data = True

    try:
        if st.session_state.loaded_dfs:

            if year_data:
                if st.session_state.cleaned:
                    st.selectbox('What dataframe would you like to view?', tuple(years), key = 'option', on_change=reset_sorted)
                    show_team_data()
                else:
                    st.subheader("Please clean the data first")
            else:
                st.subheader(f"Stats from {years[0]}-{years[len(years) - 1]}", divider = 'gray')
                show_year_data()

    except Exception as e:
        print(f"No dataframe extracted: {e}")



