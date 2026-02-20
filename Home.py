# pylint: disable=import-error
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import pandas as pd
from src.extract.nflreadpy_extract import *
from src.transform import cleaning, fe_module
from src.load.load_module import DataLoader
from src.load.load_module import get_engine
from dotenv import load_dotenv
from datetime import datetime
import os


load_dotenv()
load = DataLoader()
engine = get_engine()

# Simple streamlit homepage to look at dataframes

# In terminal call 'streamlit run Home.py'

st.set_page_config(layout="wide", page_title='NFL Analytics Platform')
st.title('🏈 NFL Offensive Analysis', text_alignment = 'center')

#SESSION STATES
if 'team_table' not in st.session_state:
    st.session_state.team_table = None

if 'game_table' not in st.session_state:
    st.session_state.game_table = None

if 'season_table' not in st.session_state:
    st.session_state.season_table = None

if 'nfl_facts_table' not in st.session_state:
    st.session_state.nfl_facts_table = None

@st.cache_data
def query_teams():
    with engine.connect() as connection:
        query = "SELECT * FROM team"
        result = pd.read_sql(query, connection)
        return result
    
@st.cache_data
def query_seasons():
    with engine.connect() as connection:
        query = "SELECT * FROM season"
        result = pd.read_sql(query, connection)
        return result
    
@st.cache_data
def query_games():
    with engine.connect() as connection:
        query = "SELECT * FROM game"
        result = pd.read_sql(query, connection)
        return result
    
@st.cache_data
def query_nfl_facts():
    with engine.connect() as connection:
        query = "SELECT * FROM nfl_facts"
        result = pd.read_sql(query, connection)
        return result
    
def get_season_view(df, start_year, end_year):
    filtered = df[
        (df["season_id"] >= start_year) &
        (df["season_id"] <= end_year)
    ]

    season_df = (
        filtered.groupby("season_id")
        .mean(numeric_only=True)
        .reset_index()
    )

    return season_df


def get_team_view(df, year):
    filtered = df[df["season_id"] == year]

    team_df = (
        filtered.groupby("team_id")
        .sum(numeric_only=True)
        .reset_index()
    )

    # Optional: calculate win %
    if "wins" in team_df.columns and "losses" in team_df.columns:
        team_df["win_pct"] = (
            team_df["wins"] /
            (team_df["wins"] + team_df["losses"])
        )

    return team_df


def get_game_view(df, year, week):
    game_df = df[
        (df["season_id"] == year) &
        (df["week"] == week)
    ].copy()

    return game_df
 

def chart_builder(df):
    
    if df.empty:
        st.warning("No data available for selected filters.")
        return

    numeric_cols = df.select_dtypes(include="number").columns.tolist()

    x_axis = st.selectbox("Select X-Axis", df.columns)
    y_axis = st.selectbox("Select Y-Axis", numeric_cols)

    graph_type = st.selectbox(
        "Select Chart Type",
        ["Bar", "Line", "Scatter", "Histogram"]
    )

    fig, ax = plt.subplots()

    if graph_type == "Bar":
        df.plot(kind="bar", x=x_axis, y=y_axis, ax=ax)

    elif graph_type == "Line":
        df.plot(kind="line", x=x_axis, y=y_axis, ax=ax)

    elif graph_type == "Scatter":
        df.plot(kind="scatter", x=x_axis, y=y_axis, ax=ax)

    elif graph_type == "Histogram":
        df[y_axis].plot(kind="hist", ax=ax)

    return fig

tab1, tab2 = st.tabs(['Chart Builder', 'Data Preview'])

with tab1:
    col1, col2 = st.columns([3, 1], border = True)

    with col2:
        option = st.selectbox(label = 'Select data you wish to chart', options = ['Season', 'Team', 'Game'])

        if st.session_state.nfl_facts_table is not None:
            nfl_facts = st.session_state.nfl_facts_table

            if option == "Season":
                start_year, end_year = st.slider(
                    "Select Year Range",
                    int(nfl_facts["season_id"].min()),
                    int(nfl_facts["season_id"].max()),
                    (2015, 2025)
                )

                df_view = get_season_view(nfl_facts, start_year, end_year)
            elif option == "Team":
                year = st.selectbox(
                    "Select Season",
                    sorted(nfl_facts["season_id"].unique())
                )

                df_view = get_team_view(nfl_facts, year)
            elif option == "Game":
                year = st.selectbox(
                    "Select Season",
                    sorted(nfl_facts["season_id"].unique())
                )

                week = st.slider("Select Week", 1, 22, 1)

                df_view = get_game_view(nfl_facts, year, week)

            chart = chart_builder(df_view)

            with col1:
                st.pyplot(chart)

with tab2:
    table1, table2, table3, table4 = st.tabs(['team_table', 'season_table', 'game_table', 'nfl_facts_table'])
    with table1:
        team = query_teams()
        st.dataframe(team, width = 'content')
        if st.session_state.team_table is None:
            st.session_state.team_table = team
    with table2:
        season = query_seasons()
        st.dataframe(season, width = 'content')
        if st.session_state.season_table is None:
            st.session_state.season_table = season
    with table3:
        game = query_games()
        st.dataframe(game, width = 'content')
        if st.session_state.game_table is None:
            st.session_state.game_table = game
    with table4:
        nfl_facts = query_nfl_facts()
        st.dataframe(nfl_facts, width = 'content')
        if st.session_state.nfl_facts_table is None:
            st.session_state.nfl_facts_table = nfl_facts
