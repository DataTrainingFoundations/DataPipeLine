"""Module of Streamlit UI to run the data pipeline"""
import streamlit as st
import pandas as pd
import time
from datetime import datetime
from src.extract.nflreadpy_extract import *
from src.extract.extract_module import DataExtractor
from src.transform import cleaning, fe_module
from src.load.load_module import DataLoader

load = DataLoader()

st.set_page_config(layout="wide", page_title='NFL Analytics Data Pipeline')
st.title("⚙️ Data Pipeline")

# Columns for extract buttons
col1, col2, _ = st.columns([1,1,6], gap = 'xxsmall')

# SESSION STATES
if 'stats_example' not in st.session_state:
    st.session_state.stats_example = None

if 'schedule_example' not in st.session_state:
    st.session_state.schedule_example = None


# Placeholder containers for ETL status
extract = st.empty()
transform = st.empty()
loader = st.empty()

# Function to check if uploaded files are valid
def validate_schema(stats_df: pd.DataFrame, schedule_df: pd.DataFrame):
    if st.session_state.stats_example is not None and st.session_state.schedule_example is not None:
        stats_missing = [c for c in stats_df if c not in st.session_state.stats_example.columns]
        schedule_missing = [c for c in schedule_df if c not in st.session_state.schedule_example.columns]
        return (len(stats_missing) == 0 and len(schedule_missing) == 0)

if st.session_state.updated is not None:
    st.write(f"Last updated: {st.session_state.updated}")

with col1:

    # Button to load most current data from this year to database
    if st.button("Get Current Data"):

        extract.success("📥 Extracting...")
        # EXTRACT DATA
        teams = get_teams()
        stats = get_team_stats()
        schedule = get_schedule(2025)

        extract.empty()
        transform.info("🔄 Transforming...")
        # TRANSFORM DATA
        team_table = fe_module.team_table(teams)
        game_table = fe_module.game_table(schedule)
        season_table = fe_module.season_table(stats)
        fact_table = fe_module.facts_table(stats_df = stats, schedule_df = schedule)
        cleaned_fact = cleaning.Cleaning.clean(fact_table)
        cleaned_season = cleaning.Cleaning.clean(season_table)

        transform.empty()
        loader.warning("📤 Loading to Database...")
        # LOAD DATA
        load.create_(df = team_table, table_name = 'team', primary_key = 'team_id')
        load.create_(df = cleaned_season, table_name = 'season', primary_key = 'season_id')
        load.create_(df = game_table, table_name = 'game', primary_key = 'game_id')
        load.create_(df = cleaned_fact, table_name = 'nfl_facts', primary_key = 'game_id')

        load.insert_(df = team_table, table_name= 'team', primary_key = 'team_id')
        load.insert_(df = cleaned_season, table_name= 'season', primary_key = 'season_id')
        load.insert_(df = game_table, table_name= 'game', primary_key = 'game_id')
        load.insert_(df = cleaned_fact, table_name= 'nfl_facts', primary_key = 'game_id')
        loader.success("✅ Data Successfully Loaded!")
        st.session_state.updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.rerun()


with col2:
    # Button to load all data available(1999-Last Year) to database
    if st.button("Load ALL Data"):

        extract.success("📥 Extracting...")
        teams = get_teams()

        # EXTRACT DATA
        dataframes = {}
        for year in range(1999, 2025):
            stats_df = get_team_stats(year)
            schedule_df = get_schedule(year)
            dataframes.setdefault(year, [stats_df, schedule_df])

        extract.empty()
        transform.info("🔄 Transforming...")
        team_table = fe_module.team_table(teams)

        # TRANSFORM DATA
        tables = {}
        for year, items in dataframes.items():
            game_table = fe_module.game_table(items[1])
            season_table = fe_module.season_table(items[0])
            fact_table = fe_module.facts_table(stats_df = items[0], schedule_df = items[1])
            cleaned_fact = cleaning.Cleaning.clean(fact_table)
            cleaned_season = cleaning.Cleaning.clean(season_table)
            tables.setdefault(year, [cleaned_season, game_table, cleaned_fact])

        transform.empty()
        loader.warning("📤 Loading to Database...")

        # LOAD DATA
        load.create_(df = team_table, table_name = 'team', primary_key = 'team_id')
        load.insert_(df = team_table, table_name= 'team', primary_key = 'team_id')

        load.create_(df = tables[1999][0], table_name = 'season', primary_key = 'season_id')
        load.create_(df = tables[1999][1], table_name = 'game', primary_key = 'game_id')
        load.create_(df = tables[1999][2], table_name = 'nfl_facts', primary_key = 'game_id')

        for items in tables.values():
            print(items[0])
            load.insert_(df = items[0], table_name= 'season', primary_key = 'season_id')
            load.insert_(df = items[1], table_name= 'game', primary_key = 'game_id')
            load.insert_(df = items[2], table_name= 'nfl_facts', primary_key = 'game_id')

        loader.success("✅ Data Successfully Loaded!")
        st.session_state.updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.rerun()


# FILE UPLOADERS
uploaded_stats = st.file_uploader(
    "Upload CSV or JSON obtained from nflfastR team_stats in the format below",
    type=["csv", "json"]
)
uploaded_schedule = st.file_uploader(
    "Upload CSV or JSON obtained from nflfastR schedule in the format below",
    type=["csv", "json"]
)

# Warning to make sure both files are uploaded
if (uploaded_stats and not uploaded_schedule) or (uploaded_schedule and not uploaded_stats):
    st.warning("Please upload BOTH stats and schedule files.")

if uploaded_stats and uploaded_schedule and validate_schema(uploaded_stats, uploaded_schedule):

    # Extract data from csv's and upload data to database
    extract.success("📥 Extracting...")
    time.sleep(1)

    # EXTRACT DATA
    stats = DataExtractor.extract_data(uploaded_stats, uploaded_stats.name)
    schedule = DataExtractor.extract_data(uploaded_schedule, uploaded_schedule.name)

    extract.empty()
    transform.info("🔄 Transforming...")
    time.sleep(1)

    # TRANSFROM DATA
    game_table = fe_module.game_table(schedule)
    season_table = fe_module.season_table(stats)
    fact_table = fe_module.facts_table(stats_df = stats, schedule_df = schedule)
    cleaned_fact = cleaning.Cleaning.clean(fact_table)
    cleaned_season = cleaning.Cleaning.clean(season_table)

    transform.empty()
    loader.warning("📤 Loading to Database...")
    time.sleep(1)
    
    # LOAD DATA
    load.create_(df = cleaned_season, table_name = 'season', primary_key = 'season_id')
    load.create_(df = game_table, table_name = 'game', primary_key = 'game_id')
    load.create_(df = cleaned_fact, table_name = 'nfl_facts', primary_key = 'game_id')

    load.insert_(df = cleaned_season, table_name= 'season', primary_key = 'season_id')
    load.insert_(df = game_table, table_name= 'game', primary_key = 'game_id')
    load.insert_(df = cleaned_fact, table_name= 'nfl_facts', primary_key = 'game_id')

    loader.success("✅ Data Successfully Loaded!")
    st.session_state.updated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    st.rerun()
elif uploaded_stats and uploaded_schedule and validate_schema(uploaded_stats, uploaded_schedule) is not True:
    st.warning("Files not in correct format")

# Example files for upload
tab1, tab2 = st.tabs(["team_stats", "schedule"])
with tab1:
    df = pd.read_csv('https://raw.githubusercontent.com/DataTrainingFoundations/DataPipeLine/refs/heads/master/2024_team_stats_example.csv')
    st.session_state.stats_example = df
    st.dataframe(df)
with tab2:
    df = pd.read_csv('https://raw.githubusercontent.com/DataTrainingFoundations/DataPipeLine/refs/heads/master/2024_schedule_example.csv')
    st.session_state.schedule_example = df
    st.dataframe(df)