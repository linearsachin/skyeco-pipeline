import streamlit as st
import duckdb
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

# Page Config
st.set_page_config(page_title="SkyEco Dashboard", page_icon="✈️", layout="wide")

st.title("✈️ SkyEco: Real-Time Flight & Weather Emissions Tracker")
st.markdown("""
This dashboard monitors live flights and correlates emissions with **OpenWeather** data. 
Processed via **Kafka**, **MotherDuck**, and **dbt**.
""")

# 1. Connect to MotherDuck
@st.cache_data(ttl=60) 
def get_data():
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
        air_temp_c,
        wind_speed_mps,
        weather_desc,
        observed_at
    FROM skyeco_dev.main.stg_flights
    ORDER BY observed_at DESC
    """
    return con.execute(query).df()

try:
    df = get_data()
    df['co2_kg_per_km'] = df['co2_kg_per_km'].fillna(0)
    df['air_temp_c'] = df['air_temp_c'].fillna(15.0)
    df['wind_speed_mps'] = df['wind_speed_mps'].fillna(0)

    # Optional: Filter out rows that are still missing critical GPS data
    df = df.dropna(subset=['latitude', 'longitude'])
    # --- Metrics Bar ---
    # Expanded to 4 columns to include weather info
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Active Flights", len(df))
    m2.metric("Avg CO2 (kg/km)", f"{df['co2_kg_per_km'].mean():.2f}")
    m3.metric("Avg Air Temp", f"{df['air_temp_c'].mean():.1f} °C")
    m4.metric("Avg Wind Speed", f"{df['wind_speed_mps'].mean():.1f} m/s")

    # --- Main Layout ---
    col_left, col_right = st.columns([2, 1])

    with col_left:
        st.subheader("🌍 Real-Time Flight Map")
        st.map(df)

    with col_right:
        st.subheader("💨 Weather vs. Efficiency")
        # Showing how wind speed correlates with calculated CO2
        st.scatter_chart(
            data=df,
            x='wind_speed_mps',
            y='co2_kg_per_km',
            color='#FF4B4B',
            use_container_width=True
        )

    # --- Data Table Section ---
    st.subheader("📊 Detailed Flight & Environmental Data")
    # Clean up the display for the table
    display_df = df[['callsign', 'origin_country', 'speed_kmh', 'altitude_m', 'air_temp_c', 'wind_speed_mps', 'weather_desc', 'co2_kg_per_km']]
    st.dataframe(display_df, use_container_width=True)

except Exception as e:
    st.error(f"Error loading dashboard: {e}")
    st.info("💡 Hint: If you just added weather columns, make sure you ran 'dbt run' and dropped the old 'raw_flights' table once.")