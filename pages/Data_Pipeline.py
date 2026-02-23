import streamlit as st
import pandas as pd
import time
from datetime import datetime
from src.extract.nflreadpy_extract import *
from src.extract.extract_module import DataExtractor
from src.transform import cleaning, fe_module
from src.load.load_module import DataLoader

load = DataLoader()

st.title("⚙️ Data Pipeline")

col1, col2, _ = st.columns([1,1,6], gap = 'xxsmall')

def validate_schema(df: pd.DataFrame):
    missing = [c for c in st.session_state.stat_example.columns if c not in df.columns]
    return (len(missing)) == 0

with col1:
    if st.button("Get Current Data"):
        #EXTRACT DATA
        teams = get_teams()
        stats = get_team_stats()
        schedule = get_schedule(2025)

        #TRANSFORM DATA
        team_table = fe_module.team_table(teams)
        game_table = fe_module.game_table(schedule)
        season_table = fe_module.season_table(stats)
        fact_table = fe_module.facts_table(stats_df = stats, schedule_df = schedule)
        cleaned_fact = cleaning.cleaning.clean(fact_table)
        cleaned_season = cleaning.cleaning.clean(season_table)

        #LOAD DATA
        load.create_(df = team_table, table_name = 'team', primary_key = 'team_id')
        load.create_(df = cleaned_season, table_name = 'season', primary_key = 'season_id')
        load.create_(df = game_table, table_name = 'game', primary_key = 'game_id')
        load.create_(df = cleaned_fact, table_name = 'nfl_facts', primary_key = 'id')

        load.insert_(df = team_table, table_name= 'team', primary_key = 'team_id')
        load.insert_(df = cleaned_season, table_name= 'season', primary_key = 'season_id')
        load.insert_(df = game_table, table_name= 'game', primary_key = 'game_id')
        load.insert_(df = cleaned_fact, table_name= 'nfl_facts', primary_key = 'id')

with col2:
    if st.button("Load ALL Data"):
        teams = get_teams()
        dataframes = {}
        for year in range(1999, 2025):
            stats_df = get_team_stats(year)
            schedule_df = get_schedule(year)
            dataframes.setdefault(year, [stats_df, schedule_df])

        team_table = fe_module.team_table(teams)
        tables = {}
        for year, items in dataframes.items():
            game_table = fe_module.game_table(items[1])
            season_table = fe_module.season_table(items[0])
            fact_table = fe_module.facts_table(stats_df = items[0], schedule_df = items[1])
            cleaned_fact = cleaning.cleaning.clean(fact_table)
            cleaned_season = cleaning.cleaning.clean(season_table)
            tables.setdefault(year, [cleaned_season, game_table, cleaned_fact])

        load.create_(df = team_table, table_name = 'team', primary_key = 'team_id')
        load.insert_(df = team_table, table_name= 'team', primary_key = 'team_id')

        load.create_(df = tables[1999][0], table_name = 'season', primary_key = 'season_id')
        load.create_(df = tables[1999][1], table_name = 'game', primary_key = 'game_id')
        load.create_(df = tables[1999][2], table_name = 'nfl_facts', primary_key = 'id')

        for items in tables.values():
            print(items[0])
            load.insert_(df = items[0], table_name= 'season', primary_key = 'season_id')
            load.insert_(df = items[1], table_name= 'game', primary_key = 'game_id')
            load.insert_(df = items[2], table_name= 'nfl_facts', primary_key = 'id')



uploaded_stats = st.file_uploader(
    "Upload CSV or JSON obtained from nflfastR team_stats in the format below",
    type=["csv", "json"]
)

uploaded_schedule = st.file_uploader(
    "Upload CSV or JSON obtained from nflfastR schedule in the format below",
    type=["csv", "json"]
)

# Placeholder containers for ETL status
extract_col, transform_col, load_col = st.columns(3)

if (uploaded_stats and not uploaded_schedule) or (uploaded_schedule and not uploaded_stats):
    st.warning("Please upload BOTH stats and schedule files.")

if uploaded_stats and uploaded_schedule:

    with extract_col:
        st.success("📥 Extracting...")

    time.sleep(1)
    stats = DataExtractor.extract_data(uploaded_stats, uploaded_stats.name)
    schedule = DataExtractor.extract_data(uploaded_schedule, uploaded_schedule.name)

    with transform_col:
        st.info("🔄 Transforming...")

    time.sleep(1)
    game_table = fe_module.game_table(schedule)
    season_table = fe_module.season_table(stats)
    fact_table = fe_module.facts_table(stats_df = stats, schedule_df = schedule)
    cleaned_fact = cleaning.cleaning.clean(fact_table)
    cleaned_season = cleaning.cleaning.clean(season_table)

    with load_col:
        st.warning("📤 Loading to Database...")

    time.sleep(1)
    load.create_(df = cleaned_season, table_name = 'season', primary_key = 'season_id')
    load.create_(df = game_table, table_name = 'game', primary_key = 'game_id')
    load.create_(df = cleaned_fact, table_name = 'nfl_facts', primary_key = 'id')

    load.insert_(df = cleaned_season, table_name= 'season', primary_key = 'season_id')
    load.insert_(df = game_table, table_name= 'game', primary_key = 'game_id')
    load.insert_(df = cleaned_fact, table_name= 'nfl_facts', primary_key = 'id')

    st.success("✅ Data Successfully Loaded!")

tab1, tab2 = st.tabs(["team_stats", "schedule"])

with tab1:
    df = pd.read_csv('https://raw.githubusercontent.com/DataTrainingFoundations/DataPipeLine/refs/heads/master/2024_team_stats_example.csv')
    st.dataframe(df)
with tab2:
    df = pd.read_csv('https://raw.githubusercontent.com/DataTrainingFoundations/DataPipeLine/refs/heads/master/2024_schedule_example.csv')
    st.dataframe(df)