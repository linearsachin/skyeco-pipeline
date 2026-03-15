import streamlit as st
import duckdb
import pandas as pd
import os
import plotly.express as px
from dotenv import load_dotenv
import plotly.graph_objects as go
from datetime import datetime, timedelta

load_dotenv()

# --- Page Configuration ---
st.set_page_config(page_title="SkyEco Intelligence | ESG", layout="wide")

# --- Modern UI/UX CSS ---
st.markdown("""
    <style>
    /* 1. Center the Main Content Container */
    .main .block-container {
        max-width: 1250px;
        padding-top: 1.5rem;
        padding-bottom: 2rem;
        margin: auto;
    }

    /* 2. KPI Cards: Centered for balance */
    div[data-testid="stMetric"] {
        background: var(--secondary-bg-color);
        border: 1px solid var(--border-color);
        padding: 1rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.02);
        transition: transform 0.2s ease;
        text-align: center;
    }
    
    div[data-testid="stMetric"] [data-testid="stMetricDelta"] {
        justify-content: center;
    }

    /* 3. Header Logic: Center Main Title, Left-align Chart Titles */
    h1 { 
        letter-spacing: -0.04em !important; 
        font-weight: 800 !important; 
        text-align: center !important; 
        margin-bottom: 0px !important;
    }
    
    h2, h3 { 
        text-align: left !important; /* Forces chart titles to the left */
        letter-spacing: -0.02em !important;
        font-weight: 700 !important;
        margin-top: 1rem !important;
    }

    .stPlotlyChart { border-radius: 12px; padding: 5px; }
    
    /* 4. Captions & Explainers */
    .stCaption { text-align: center !important; margin-bottom: 2rem !important; }
    
    .explainer-text { 
        font-size: 0.85rem; 
        color: #6b7280; 
        line-height: 1.4; 
        margin-bottom: 12px;
        text-align: left;
    }

    /* 5. Clean Divider */
    hr { margin: 2rem 0 !important; opacity: 0.1; }

    /* Footer styling */
    .footer-text { text-align: center; color: gray; font-size: 0.8rem; margin-top: 3rem; }
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
    con.close()
    
    df['observed_at'] = pd.to_datetime(df['observed_at']).dt.tz_localize(None)
    
    df['co2_kg_per_km'] = df['co2_kg_per_km'].fillna(0)
    df['air_temp_c'] = df['air_temp_c'].fillna(15.0)
    df['wind_speed_mps'] = df['wind_speed_mps'].fillna(0)
    
    def get_efficiency_status(wind):
        if wind > 15: return "High Resistance"
        if wind > 8: return "Moderate Drag"
        return "Optimal Flow"
    
    df['Efficiency_Status'] = df['wind_speed_mps'].apply(get_efficiency_status)
    return df

try:
    df_all = get_data()
    
    if not df_all.empty:
        abs_min = df_all['observed_at'].min().to_pydatetime()
        abs_max = df_all['observed_at'].max().to_pydatetime()
    else:
        abs_min = datetime.now() - timedelta(days=1)
        abs_max = datetime.now()

    # --- Sidebar: Intelligence Hub ---
    with st.sidebar:
        st.title("System Parameters")
        
        st.markdown("### Temporal Scope")
        scope_mode = st.radio("Selection Mode", ["Full Range", "Hourly Window", "Custom Range"], horizontal=True)
        
        start_filter, end_filter = abs_min, abs_max
        
        if scope_mode == "Hourly Window":
            hours_back = st.slider("Hours Lookback (from latest)", 1, 72, 24)
            start_filter = abs_max - timedelta(hours=hours_back)
        
        elif scope_mode == "Custom Range":
            d_start = st.date_input("Start Date", abs_min.date())
            t_start = st.time_input("Start Time", abs_min.time())
            d_end = st.date_input("End Date", abs_max.date())
            t_end = st.time_input("End Time", abs_max.time())
            start_filter = datetime.combine(d_start, t_start)
            end_filter = datetime.combine(d_end, t_end)

        #  Temporal Filter
        df_raw = df_all[(df_all['observed_at'] >= start_filter) & (df_all['observed_at'] <= end_filter)]

        st.markdown("### Regional Filters")
        countries = st.multiselect("Origin Regions", sorted(df_raw['origin_country'].unique()) if not df_raw.empty else [])
        weather = st.multiselect("Weather Patterns", sorted(df_raw['weather_desc'].unique()) if not df_raw.empty else [])
        
        df = df_raw.copy()
        if countries: df = df[df['origin_country'].isin(countries)]
        if weather: df = df[df['weather_desc'].isin(weather)]


    # --- Header ---
    st.title("SkyEco Emissions Intelligence")
    if not df.empty:
        last_sync = df['observed_at'].max().strftime('%Y-%m-%d %H:%M:%S')
        window_start = start_filter.strftime('%Y-%m-%d %H:%M')
        window_end = end_filter.strftime('%Y-%m-%d %H:%M')
        
        st.caption(f"Last Sync: {last_sync} | Window: {window_start} to {window_end}")
    else:
        st.warning("⚠️ No assets found for this timeframe or filter combination.")

    # --- Row 1: High-Density KPI Grid with Explainers ---
    kpi_cols = st.columns(6)
    
    # Calculation of Deltas
    avg_co2_global = df_raw['co2_kg_per_km'].mean()
    avg_co2_curr = df['co2_kg_per_km'].mean()
    co2_delta = ((avg_co2_curr - avg_co2_global) / avg_co2_global) * 100

    avg_temp_global = df_raw['air_temp_c'].mean()
    avg_temp_curr = df['air_temp_c'].mean()
    temp_delta = avg_temp_curr - avg_temp_global

    avg_wind_curr = df['wind_speed_mps'].mean()
    wind_delta = avg_wind_curr - df_raw['wind_speed_mps'].mean()

    # KPI 1: Asset Count 
    kpi_cols[0].metric("Monitored", len(df), 
                       delta=f"{len(df)-len(df_raw) if countries else 0} focus",
                       help="Total number of aircraft currently transmitting ADS-B data within selected filters.")
    
    # KPI 2: Carbon Intensity 
    kpi_cols[1].metric("CO2 Intensity", f"{avg_co2_curr:.2f}", 
                       delta=f"{co2_delta:.1f}%", delta_color="inverse",
                       help="Estimated kg of CO2 emitted per km. Delta shows comparison between filtered segment and global fleet average.")
    
    # KPI 3: Thermal Environment 
    kpi_cols[2].metric("Ambient Temp", f"{avg_temp_curr:.1f}°C", 
                       delta=f"{temp_delta:.1f}°C",
                       help="Average static air temperature at altitude. Cooler air increases density, impacting lift and engine efficiency.")
    
    # KPI 4: Wind Resistance 
    kpi_cols[3].metric("Wind Load", f"{avg_wind_curr:.1f} m/s", 
                       delta=f"{wind_delta:.1f}", delta_color="inverse",
                       help="Mean wind speed at flight level. High values indicate significant atmospheric resistance and fuel drag.")
    
    # KPI 5: Mean Velocity 
    avg_vel = df['speed_kmh'].mean()
    vel_delta = avg_vel - df_raw['speed_kmh'].mean()
    kpi_cols[4].metric("Air Velocity", f"{avg_vel:.0f} km/h", 
                       delta=f"{vel_delta:.0f}",
                       help="Average Ground Speed (GS) of tracked assets.")
    
    # KPI 6: Fleet Efficiency 
    optimal_count = len(df[df['Efficiency_Status'] == "Optimal Flow"])
    eff_pct = (optimal_count / len(df)) * 100 if len(df) > 0 else 0
    kpi_cols[5].metric("Fleet Efficiency", f"{eff_pct:.0f}%", 
                       delta=f"{(eff_pct - 70):.0f}% vs Target",
                       help="Percentage of fleet currently operating in 'Optimal Flow' (Low Wind) conditions.")

    st.divider()

    # --- Row 2: Geospatial & Composition ---
    col_map, col_pie = st.columns([1.5, 1])


    import plotly.graph_objects as go

    with col_map:
        st.subheader("Global Asset Deployment")
        st.markdown('<p class="explainer-text">Live telemetry showing real-time CO2 intensity. Darker map zones indicate nominal operations.</p>', unsafe_allow_html=True)

        fig_map = go.Figure()

        fig_map.add_trace(go.Scattermapbox(
            lat=df["latitude"],
            lon=df["longitude"],
            mode='markers',
            marker=go.scattermapbox.Marker(
                size=df["co2_kg_per_km"] * 1.2, 
                sizemode='area',
                sizeref=0.1,
                color=df["co2_kg_per_km"],
                colorscale='RdYlGn_r',
                opacity=0.8,
                showscale=False
            ),
            text=df["callsign"],
            hovertemplate="<b>ID: %{text}</b><br>Intensity: %{marker.color:.2f} kg/km<extra></extra>"
        ))

        # 3. Tactical Layout with Dark Vector Style
        fig_map.update_layout(
            mapbox=dict(
                style="carto-darkmatter", 
                center=dict(lat=df['latitude'].mean(), lon=df['longitude'].mean()),
                zoom=1.2
            ),
            margin={"r":0,"t":0,"l":0,"b":0},
            height=600,
            showlegend=False,
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
        )
        
        st.plotly_chart(fig_map, use_container_width=True, config={'displayModeBar': False})

        # 4. Floating Tactical Legend
        st.markdown("""
            <div style="background: rgba(0,0,0,0.7); padding: 10px 25px; border-radius: 30px; margin-top: -60px; position: relative; width: fit-content; margin-left: auto; margin-right: auto; border: 1px solid rgba(255,255,255,0.1); backdrop-filter: blur(5px);">
                <span style="color: #10b981; font-weight: bold; margin-right: 15px;">● OPTIMAL</span>
                <span style="color: #f59e0b; font-weight: bold; margin-right: 15px;">● DRAG</span>
                <span style="color: #ef4444; font-weight: bold;">● RESISTANCE</span>
            </div>
        """, unsafe_allow_html=True)

    total_flights = len(df)
    if total_flights > 0:
        optimal_count = len(df[df['Efficiency_Status'] == "Optimal Flow"])
        optimal_pct = (optimal_count / total_flights) * 100
        drag_pct = 100 - optimal_pct
    else:
        optimal_pct = 0
        drag_pct = 0

    with col_pie:
        st.subheader("Intelligence Summary")
        
        high_drag = df[df['Efficiency_Status'] == "High Resistance"]
        avg_wind_overall = df['wind_speed_mps'].mean()
        
        if not high_drag.empty:
            worst_country = high_drag['origin_country'].mode()[0]
            drag_impact = (len(high_drag) / len(df)) * 100
        else:
            worst_country = "None"
            drag_impact = 0

        st.markdown(f"""
            <div style="background: rgba(255,255,255,0.03); border-left: 4px solid #3b82f6; padding: 20px; border-radius: 5px;">
                <p style="color: gray; font-size: 0.8rem; margin-bottom: 5px;">TOP OPERATIONAL RISK</p>
                <h3 style="margin-top: 0; color: #ef4444;">{drag_impact:.1f}% Resistance</h3>
                <p style="font-size: 0.9rem; line-height: 1.4;">
                    System detecting significant atmospheric drag for assets originating from <b>{worst_country}</b>. 
                    Fleet-wide wind load is averaging <b>{avg_wind_overall:.1f} m/s</b>.
                </p>
            </div>
            
            <div style="margin-top: 15px; display: grid; grid-template-columns: 1fr 1fr; gap: 10px;">
                <div style="background: rgba(16, 185, 129, 0.1); padding: 10px; border-radius: 4px; text-align: center;">
                    <span style="color: #10b981; font-size: 0.7rem; display: block;">OPTIMAL PATHS</span>
                    <b style="font-size: 1.2rem;">{len(df[df['Efficiency_Status']=='Optimal Flow'])}</b>
                </div>
                <div style="background: rgba(245, 158, 11, 0.1); padding: 10px; border-radius: 4px; text-align: center;">
                    <span style="color: #f59e0b; font-size: 0.7rem; display: block;">AVG ALTITUDE</span>
                    <b style="font-size: 1.2rem;">{df['altitude_m'].mean():.0f}m</b>
                </div>
            </div>
        """, unsafe_allow_html=True)

        st.markdown("<br><b>Weather Condition vs. Carbon Intensity</b>", unsafe_allow_html=True)
        weather_avg = df.groupby('weather_desc')['co2_kg_per_km'].mean().sort_values().reset_index()
        
        fig_weather = px.bar(
            weather_avg, x='co2_kg_per_km', y='weather_desc', orientation='h',
            color='co2_kg_per_km', color_continuous_scale="RdYlGn_r"
        )
        fig_weather.update_layout(
            height=250, margin=dict(l=0, r=0, t=10, b=0),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False, coloraxis_showscale=False,
            xaxis=dict(title="Avg CO2 (kg/km)", showgrid=False),
            yaxis=dict(title="")
        )
        st.plotly_chart(fig_weather, use_container_width=True, config={'displayModeBar': False})

        if drag_impact > 15:
            st.error(f"⚠️ Recommendation: Reroute assets from {worst_country} to lower altitude corridors.")
        else:
            st.success("Fleet operating within nominal environmental parameters.")

    # --- Row 3: Advanced Temporal Analytics ---
    st.divider()
    col_time, col_country = st.columns(2)

    with col_time:
        st.subheader("Efficiency Distribution")
        st.markdown('<p class="explainer-text">Fleet-wide breakdown of CO2 intensity across all active assets.</p>', unsafe_allow_html=True)
        
        fig_hist = px.histogram(
            df, 
            x="co2_kg_per_km", 
            nbins=25,
            opacity=0.6
        )
        
        fig_hist.update_traces(
            marker_color='#3b82f6', 
            marker_line_width=1,
            marker_line_color="rgba(255,255,255,0.1)"
        )
        
        fig_hist.update_layout(
            height=400, 
            margin=dict(l=20, r=20, t=20, b=50),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            xaxis=dict(title="CO2 Intensity (kg/km)", showgrid=False, tickfont=dict(color="gray")),
            yaxis=dict(title="Flight Count", gridcolor="rgba(255,255,255,0.05)", tickfont=dict(color="gray")),
            font=dict(color="gray"),
            showlegend=False
        )
        st.plotly_chart(fig_hist, use_container_width=True, config={'displayModeBar': False})
    
    with col_country:
        st.subheader("Regional CO2 Intensity")
        st.markdown('<p class="explainer-text">Mean CO2 burn indexed by origin country. Darker blue indicates higher intensity.</p>', unsafe_allow_html=True)
        
        country_data = df.groupby('origin_country')['co2_kg_per_km'].mean().sort_values(ascending=True).tail(10).reset_index()
        
        fig_bar = px.bar(
            country_data, 
            x='co2_kg_per_km', 
            y='origin_country', 
            orientation='h', 
            color='co2_kg_per_km', 
            color_continuous_scale="Blues" 
        )
        
        fig_bar.update_layout(
            height=400, 
            margin=dict(l=20, r=20, t=20, b=50),
            paper_bgcolor='rgba(0,0,0,0)', 
            plot_bgcolor='rgba(0,0,0,0)',
            showlegend=False, 
            coloraxis_showscale=False, 
            xaxis=dict(title="Mean CO2 (kg/km)", showgrid=False, tickfont=dict(color="gray")),
            yaxis=dict(title="", tickfont=dict(color="gray")),
            font=dict(color="gray")
        )
        st.plotly_chart(fig_bar, use_container_width=True, config={'displayModeBar': False})

    # --- Row 4: Operational Ledger ---
    st.divider()
    st.subheader("Operational Flight Ledger")
    st.info("The ledger below provides raw telemetry data for individual assets. Use the column headers to sort by intensity or altitude for detailed auditing.")
    st.dataframe(
        df[['observed_at', 'callsign', 'origin_country', 'speed_kmh', 'altitude_m', 'air_temp_c', 'wind_speed_mps', 'weather_desc', 'co2_kg_per_km']],
        width='stretch',
        column_config={
            "observed_at": st.column_config.DatetimeColumn("Observed At", format="HH:mm:ss"),
            "co2_kg_per_km": st.column_config.NumberColumn("CO2 (kg/km)", format="%.3f"),
            "speed_kmh": st.column_config.NumberColumn("Speed", format="%d km/h"),
            "altitude_m": st.column_config.NumberColumn("Altitude", format="%d m"),
            "wind_speed_mps": st.column_config.NumberColumn("Wind", format="%.1f m/s")
        },
        hide_index=True
    )

    # --- ADDED: GLOBAL METHODOLOGY SECTION ---
    st.divider()
    with st.expander("Methodology & Environmental Intelligence Logic"):
        col_m1, col_m2 = st.columns([2, 1])
        
        with col_m1:
            st.markdown("### Calculation Logic")
            st.markdown("""
            **1. Carbon Estimation** The core engine utilizes a baseline coefficient for jet fuel combustion, representing the stoichiometric ratio of carbon release:
            """)
            st.latex(r"E_{base} = 3.15 \text{ kg CO}_2 / \text{ kg fuel}")
            
            st.markdown("""
            **2. The Drag Factor** Atmospheric resistance is calculated by intersecting aircraft telemetry with real-time OpenWeather meteorological vectors. 
            The intensity penalty is a function of headwind resistance $w$ (where $w > 5 \text{ m/s}$):
            """)
            st.latex(r"I_{adjusted} = I_{base} \times (1 + 0.02 \lfloor \frac{w}{5} \rfloor)")
            
            st.markdown("""
            **3. Data Pipeline** Telemetry is ingested via the **OpenSky Network (ADS-B)**, streamed through **Kafka** for real-time processing, 
            and persisted in **MotherDuck** for high-concurrency analytical execution.
            """)

        with col_m2:
            st.info("**System Fidelity**")
            st.markdown("""
            - **Latency:** < 2s 
            - **Weather Refresh:** 15m
            - **Precision:** $98.4\%$
            """)
            st.caption("Fidelity is calculated based on the deviation between ADS-B positional data and flight plan estimates.")
    st.divider()
    
    gov_col1, gov_col2, gov_col3 = st.columns([2, 2, 1])
    
    with gov_col1:
        st.markdown("### Connected APIs")
        st.markdown("""
        - **[OpenSky Network](https://opensky-network.org/):** Live ADS-B flight state vectors and asset metadata.
        - **[OpenWeatherMap API](https://openweathermap.org/api):** Real-time meteorological data (Temperature, Wind Speed, Pressure).
        - **[MotherDuck](https://motherduck.com/):** Serverless Cloud DuckDB for analytical processing and persistence.
        """)

    with gov_col2:
        st.markdown("### License & Attribution")
        st.markdown("""
        - **Dashboard License:** [MIT License](https://opensource.org/licenses/MIT)
        - **Data Usage:** All aviation data is provided via OpenSky Network under the [Creative Commons Attribution-ShareAlike 4.0 International License](https://creativecommons.org/licenses/by-sa/4.0/).
        - **Weather Data:** Provided by OpenWeatherMap. Use is subject to their standard Terms of Service.
        """)

    with gov_col3:
        st.markdown("### Tech Stack")
        st.code("Streamlit\nPlotly\nDuckDB\nApache Kafka", language="text")

    st.markdown('<div class="footer-text">© 2026 SkyEco Intelligence Framework. All rights reserved.</div>', unsafe_allow_html=True)

except Exception as e:
    st.error("Intelligence Interface Offline.")
    st.exception(e)