import streamlit as st
import duckdb
import pandas as pd
import os
import plotly.express as px
from dotenv import load_dotenv

load_dotenv()

# --- Page Configuration ---
st.set_page_config(page_title="SkyEco Intelligence", layout="wide")

# --- CSS for Theme Fluidity ---
st.markdown("""
    <style>
    div[data-testid="stMetric"] {
        background-color: var(--secondary-bg-color);
        border: 1px solid var(--border-color);
        padding: 1.2rem;
        border-radius: 0.5rem;
    }
    h1, h2, h3 { letter-spacing: -0.03em; font-weight: 700; }
    .stPlotlyChart { border-radius: 8px; }
    </style>
    """, unsafe_allow_html=True)

# --- Data Engine ---
@st.cache_data(ttl=60)
def get_data():
    token = os.getenv('MOTHERDUCK_TOKEN')
    con = duckdb.connect(f"md:skyeco_dev?motherduck_token={token}")
    
    query = """
    SELECT 
        icao24, callsign, origin_country, latitude, longitude, 
        altitude_m, speed_kmh, co2_kg_per_km, air_temp_c, 
        wind_speed_mps, weather_desc, observed_at
    FROM skyeco_dev.main.stg_flights
    ORDER BY observed_at DESC
    """
    df = con.execute(query).df()
    
    # Defaults & Cleaning
    df['co2_kg_per_km'] = df['co2_kg_per_km'].fillna(0)
    df['air_temp_c'] = df['air_temp_c'].fillna(15.0)
    df['wind_speed_mps'] = df['wind_speed_mps'].fillna(0)
    df['observed_at'] = pd.to_datetime(df['observed_at'])
    
    def get_efficiency_status(wind):
        if wind > 15: return "Sub-Optimal (Drag)"
        if wind > 8: return "Moderate Resistance"
        return "Optimal Efficiency"
    
    df['Efficiency_Status'] = df['wind_speed_mps'].apply(get_efficiency_status)
    return df

try:
    df_raw = get_data()

    # --- Sidebar: System Intelligence ---
    with st.sidebar:
        st.title("Network Parameters")
        
        # System Info Box
        with st.container(border=True):
            st.markdown("**System Metadata**")
            st.write(f"🌍 Unique Countries: `{df_raw['origin_country'].nunique()}`")
            st.write(f"💨 Avg Velocity: `{df_raw['speed_kmh'].mean():.1f} km/h`")
            st.write(f"✈️ Active Assets: `{len(df_raw)}`")
        
        st.markdown("---")
        countries = st.multiselect("Origin Regions", sorted(df_raw['origin_country'].unique()))
        weather = st.multiselect("Weather Patterns", sorted(df_raw['weather_desc'].unique()))
        
        df = df_raw.copy()
        if countries: df = df[df['origin_country'].isin(countries)]
        if weather: df = df[df['weather_desc'].isin(weather)]

    # --- Header ---
    st.title("SkyEco Emissions Intelligence")
    st.caption(f"Network Status: Synchronized • Last Update: {df['observed_at'].max()}")

    # --- Row 1: Operational Metrics ---
    m1, m2, m3, m4 = st.columns(4)
    current_avg_co2 = df['co2_kg_per_km'].mean()
    
    m1.metric("Monitored Flights", len(df))
    m2.metric("Avg Intensity", f"{current_avg_co2:.2f} kg/km")
    m3.metric("Air Velocity", f"{df['speed_kmh'].mean():.0f} km/h")
    m4.metric("Avg Wind", f"{df['wind_speed_mps'].mean():.1f} m/s")

    st.markdown("---")

    # --- Row 2: Geospatial & Efficiency Composition ---
    col_map, col_pie = st.columns([2, 1])

    with col_map:
        st.subheader("Global Deployment Map")
        fig_map = px.scatter_map(
            df, lat="latitude", lon="longitude", color="co2_kg_per_km", 
            size="speed_kmh", hover_name="callsign", color_continuous_scale="RdYlGn_r",
            size_max=10, zoom=1
        )
        fig_map.update_layout(map_style="open-street-map", margin={"r":0,"t":0,"l":0,"b":0}, 
                              height=450, paper_bgcolor='rgba(0,0,0,0)', font=dict(color="gray"))
        st.plotly_chart(fig_map, width='stretch')

    with col_pie:
        st.subheader("Efficiency Mix")
        fig_pie = px.pie(
            df, names='Efficiency_Status', color='Efficiency_Status', hole=0.6,
            color_discrete_map={"Optimal Efficiency": "#10b981", "Moderate Resistance": "#f59e0b", "Sub-Optimal (Drag)": "#ef4444"}
        )
        fig_pie.update_layout(height=450, showlegend=True, paper_bgcolor='rgba(0,0,0,0)', 
                              legend=dict(orientation="h", yanchor="bottom", y=-0.2, font=dict(color="gray")))
        st.plotly_chart(fig_pie, width='stretch')

    st.markdown("---")

    # --- Row 3: Fleet Dynamics ---
    col_time, col_country = st.columns(2)

    with col_time:
        st.subheader("Emission Volatility (Temporal)")
        df_time = df.sort_values('observed_at')
        fig_time = px.line(df_time, x='observed_at', y='co2_kg_per_km')
        fig_time.update_traces(line_color='#3b82f6', line_width=2)
        fig_time.update_layout(height=350, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                               xaxis=dict(title="Observation Time", gridcolor="rgba(128,128,128,0.1)"),
                               yaxis=dict(title="CO2 Intensity", gridcolor="rgba(128,128,128,0.1)"),
                               font=dict(color="gray"))
        st.plotly_chart(fig_time, width='stretch')

    with col_country:
        st.subheader("Top Regional CO2 Contributors")
        # Aggregating total CO2 per country in the current view
        country_data = df.groupby('origin_country')['co2_kg_per_km'].mean().sort_values(ascending=False).head(10).reset_index()
        fig_bar = px.bar(country_data, x='co2_kg_per_km', y='origin_country', orientation='h', color='co2_kg_per_km', color_continuous_scale="Reds")
        fig_bar.update_layout(height=350, paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                               showlegend=False, coloraxis_showscale=False,
                               xaxis=dict(title="Mean CO2 Burn", gridcolor="rgba(128,128,128,0.1)"),
                               yaxis=dict(title="", gridcolor="rgba(128,128,128,0.1)"),
                               font=dict(color="gray"))
        st.plotly_chart(fig_bar, width='stretch')

    # --- Row 4: The Operational Ledger ---
    st.markdown("---")
    st.subheader("Operational Flight Ledger")
    st.markdown("Full audit log of active assets enriched with meteorological data.")
    
    # Stylized DataFrame
    st.dataframe(
        df[['observed_at', 'callsign', 'origin_country', 'speed_kmh', 'altitude_m', 'air_temp_c', 'wind_speed_mps', 'weather_desc', 'co2_kg_per_km']],
        width='stretch',
        column_config={
            "observed_at": st.column_config.DatetimeColumn("Timestamp", format="D MMM, HH:mm"),
            "co2_kg_per_km": st.column_config.NumberColumn("CO2 Burn", format="%.2f kg/km"),
            "speed_kmh": st.column_config.NumberColumn("Velocity", format="%d km/h"),
            "altitude_m": st.column_config.NumberColumn("Altitude", format="%d m")
        },
        hide_index=True
    )

except Exception as e:
    st.error("System Interface Disconnect.")
    st.exception(e)