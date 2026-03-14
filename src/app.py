import streamlit as st
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Page Config
st.set_page_config(page_title="SkyEco Dashboard", page_icon="✈️", layout="wide")

st.title("✈️ SkyEco: Real-Time Flight Emissions Tracker")
st.markdown("This dashboard displays real-time flight data processed through **Kafka**, **MotherDuck**, and **dbt**.")

# 1. Connect to MotherDuck
@st.cache_data(ttl=60) # Refresh data every minute
def get_data():
    md_token = os.getenv('MOTHERDUCK_TOKEN')
    # Connect to the Silver layer table created by dbt
    con = duckdb.connect(f"md:skyeco_dev")
    
    query = """
    SELECT 
        icao24, 
        callsign, 
        origin_country, 
        latitude, 
        longitude, 
        altitude_m, 
        speed_kmh, 
        co2_kg_per_km,
        observed_at
    FROM skyeco_dev.main.stg_flights
    """
    return con.execute(query).df()

try:
    df = get_data()

    # --- Metrics Bar ---
    col1, col2, col3 = st.columns(3)
    col1.metric("Active Flights Tracked", len(df))
    col2.metric("Avg CO2 (kg/km)", round(df['co2_kg_per_km'].mean(), 2))
    col3.metric("Highest Altitude (m)", int(df['altitude_m'].max()))

    # --- Map Section ---
    st.subheader("🌍 Global Flight Map")
    # Streamlit looks for 'latitude' and 'longitude' columns automatically
    st.map(df)

    # --- Data Table Section ---
    st.subheader("📊 Flight Details & Emissions")
    st.dataframe(df, use_container_width=True)

except Exception as e:
    st.error(f"Could not connect to MotherDuck: {e}")
    st.info("Ensure your MOTHERDUCK_TOKEN is set in the .env file.")