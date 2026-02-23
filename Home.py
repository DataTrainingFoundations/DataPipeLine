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

if st.session_state.updated is not None:
    st.text(f"Last updated: {st.session_state.updated}")

#SESSION STATES
if 'team_table' not in st.session_state:
    st.session_state.team_table = None

if 'game_table' not in st.session_state:
    st.session_state.game_table = None

if 'season_table' not in st.session_state:
    st.session_state.season_table = None

if 'nfl_facts_table' not in st.session_state:
    st.session_state.nfl_facts_table = None

def query_teams():
    with engine.connect() as connection:
        query = "SELECT * FROM team"
        result = pd.read_sql(query, connection)
        return result
    
def query_seasons():
    with engine.connect() as connection:
        query = "SELECT * FROM season"
        result = pd.read_sql(query, connection)
        return result
    
def query_games():
    with engine.connect() as connection:
        query = "SELECT * FROM game"
        result = pd.read_sql(query, connection)
        return result
    
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
        .agg({
            "pass_attempts": "sum",
            "rush_attempts": "sum",
            "pass_yards": "sum",
            "rush_yards": "sum",
            "pass_tds": "sum",
            "rush_tds": "sum",
        })
        .reset_index()
    )

    season_df["avg_pass_pct"] = (
        season_df["pass_attempts"] /
        (season_df["pass_attempts"] + season_df["rush_attempts"])
    )

    season_df["avg_rush_pct"] = 1 - season_df["avg_pass_pct"]
    season_df["avg_pass_pct"] *= 100
    season_df["avg_rush_pct"] *= 100

    new_season_df = season_df.dropna()
    return new_season_df


def get_team_view(df, year):
    filtered = df[df["season_id"] == year]

    # Aggregate season totals per team
    team_df = (
        filtered.groupby("team_id")
        .agg({
            "pass_attempts": "sum",
            "rush_attempts": "sum",
            "pass_yards": "sum",
            "rush_yards": "sum",
            "pass_tds": "sum",
            "rush_tds": "sum",
            "result": lambda x: list(x),  # keep results for win %
            "game_type": lambda x: list(x)
        })
        .reset_index()
    )

    team_df["made_playoffs"] = team_df["game_type"].apply(
        lambda games: any(g != "REG" for g in games)
    )

    team_df["made_superbowl"] = team_df["game_type"].apply(
        lambda games: any(g == "SB" for g in games)
    )

    def get_status(row):
        if row["made_superbowl"]:
            return "Super Bowl"
        elif row["made_playoffs"]:
            return "Playoffs"
        else:
            return "Regular Season"
        
    team_df["season_status"] = team_df.apply(get_status, axis=1)

    team_df['pass_percentage'] = (team_df['pass_attempts'] / (team_df['pass_attempts'] + team_df['rush_attempts'])) * 100
    team_df['rush_percentage'] = (team_df['rush_attempts'] / (team_df['pass_attempts'] + team_df['rush_attempts'])) * 100
    team_df['pass_td_percentage'] = (team_df['pass_tds'] / (team_df['pass_tds'] + team_df['rush_tds'])) * 100
    team_df['rush_td_percentage'] = (team_df['rush_tds'] / (team_df['pass_tds'] + team_df['rush_tds'])) * 100

    team_df.drop(columns=["game_type"], inplace=True)

    team_df["win_pct"] = team_df["result"].apply(calculate_win_pct)

    new_team_df = team_df.dropna()
    return new_team_df


def get_game_view(df, year, week):
    game_df = df[
        (df["season_id"] == year) &
        (df["week"] == week)
    ].copy()

    new_game_df = game_df.dropna()
    return new_game_df

def calculate_win_pct(results):
    wins = results.count("W")
    losses = results.count("L")
    ties = results.count("T")

    total = wins + losses + ties

    if total == 0:
        return 0

    return (wins + 0.5 * ties) / total
 

def chart_builder(df, x_axis=None, compare = False):

    if df.empty:
        st.warning("No data available for selected filters.")
        return

    if x_axis is None:
        x_axis = st.selectbox("Select X-Axis", df.columns)

    y_options = [
        col for col in df.select_dtypes(include="number").columns
        if col != x_axis
    ]

    if compare:
        y_axis = st.multiselect(
            "Select Metrics to Compare",
            y_options,
            default=y_options[:2]
        )

        if len(y_axis) < 2:
            st.warning("Select at least two metrics.")
            st.stop()
    else:
        y_axis = st.selectbox("Select Y-Axis", y_options)

    graph_type = st.selectbox(
        "Select Chart Type",
        ["Bar", "Line", "Scatter", "Histogram"]
    )

    has_status_col = "season_status" in df.columns

    color_map = {
        "Super Bowl": "#FFD700",     # gold
        "Playoffs": "#d62728",       # red
        "Regular Season": "#1f77b4"  # blue
    }

    # ---------- BAR ----------
    if graph_type == "Bar":

        if has_status_col:
            fig = px.bar(
                df,
                x=x_axis,
                y=y_axis,
                color="season_status",
                color_discrete_map=color_map,
                hover_data=df.columns
            )
        else:
            fig = px.bar(
                df,
                x=x_axis,
                y=y_axis,
                hover_data=df.columns
            )

    # ---------- LINE ----------
    elif graph_type == "Line":

        if isinstance(y_axis, list):
            fig = px.line(
                df,
                x=x_axis,
                y=y_axis,
                markers=True
            )
        else:
            fig = px.line(
                df,
                x=x_axis,
                y=y_axis,
                markers=True,
                hover_data=df.columns
            )

    # ---------- SCATTER ----------
    elif graph_type == "Scatter":

        if has_status_col:
            base_size = 14
            playoff_boost = 2

            sizes = [
                base_size + playoff_boost if playoff else base_size
                for playoff in df["made_playoffs"]
            ]

            fig = px.scatter(
                df,
                x=x_axis,
                y=y_axis,
                color="season_status",
                size=sizes,
                size_max=18,
                color_discrete_map=color_map,
                hover_data=df.columns
            )
        else:
            fig = px.scatter(
                df,
                x=x_axis,
                y=y_axis,
                hover_data=df.columns
            )

    # ---------- HISTOGRAM ----------
    elif graph_type == "Histogram":

        fig = px.histogram(
            df,
            x=y_axis
        )

    fig.update_layout(template="plotly_white")

    return fig

tab1, tab2 = st.tabs(['Data Preview', 'Chart Builder'])

with tab1:
    table1, table2, table3, table4 = st.tabs(['team_table', 'season_table', 'game_table', 'nfl_facts_table'])
    with table1:
        team = query_teams()
        st.dataframe(team, width = 'content')
        st.session_state.team_table = team
    with table2:
        season = query_seasons()
        st.dataframe(season, width = 'content')
        st.session_state.season_table = season
    with table3:
        game = query_games()
        st.dataframe(game, width = 'content')
        st.session_state.game_table = game
    with table4:
        nfl_facts = query_nfl_facts()
        st.dataframe(nfl_facts, width = 'content')
        st.session_state.nfl_facts_table = nfl_facts

with tab2:
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
                    (1999, 2025)
                )

                compare_mode = st.toggle("Compare Multiple Metrics")

                df_view = get_season_view(nfl_facts, start_year, end_year)
                chart = chart_builder(df_view, 'season_id', compare_mode)
            elif option == "Team":
                year = st.selectbox(
                    "Select Season",
                    sorted(nfl_facts["season_id"].unique())
                )

                df_view = get_team_view(nfl_facts, year)
                chart = chart_builder(df_view)
            elif option == "Game":
                year = st.selectbox(
                    "Select Season",
                    sorted(nfl_facts["season_id"].unique())
                )

                week = st.slider("Select Week", 1, 22, 1)

                df_view = get_game_view(nfl_facts, year, week)
                chart = chart_builder(df_view)

            with col1:
                st.plotly_chart(chart, width = 'content')
