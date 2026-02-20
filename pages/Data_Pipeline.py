import streamlit as st
import pandas as pd
import time

st.title("⚙️ Data Pipeline")

uploaded_file = st.file_uploader(
    "Upload CSV or JSON",
    type=["csv", "json"]
)

# Placeholder containers for ETL status
extract_col, transform_col, load_col = st.columns(3)

if uploaded_file:

    # ---------------------
    # EXTRACT
    # ---------------------
    with extract_col:
        st.success("📥 Extracting...")

    time.sleep(1)

    if uploaded_file.name.endswith(".csv"):
        df = pd.read_csv(uploaded_file)
    else:
        df = pd.read_json(uploaded_file)

    # ---------------------
    # TRANSFORM
    # ---------------------
    with transform_col:
        st.info("🔄 Transforming...")

    time.sleep(1)

    # Example transformation
    df.columns = df.columns.str.lower()

    # ---------------------
    # LOAD
    # ---------------------
    with load_col:
        st.warning("📤 Loading to Database...")

    time.sleep(1)

    # Simulated load
    st.success("✅ Data Successfully Loaded!")

    st.dataframe(df.head())