-- models/silver/stg_flights.sql
{{ config(materialized='table') }}

WITH base_data AS (
    SELECT 
        icao24,
        callsign,
        origin_country,
        latitude,
        longitude,
        -- Fallback to 0 if altitude or velocity is missing
        CAST(COALESCE(altitude, 0) AS FLOAT) as altitude_m,
        CAST(COALESCE(velocity, 0) AS FLOAT) as velocity_ms,
        
        -- --- NULL HANDLING FOR WEATHER ---
        -- Fallback to 15°C (Standard Atmosphere) if temp is null
        CAST(COALESCE(temp, 15.0) AS FLOAT) as air_temp_c,
        -- Fallback to 0 m/s if wind is null
        CAST(COALESCE(wind_speed, 0.0) AS FLOAT) as wind_speed_mps,
        -- Fallback to 'Clear' if description is null
        COALESCE(weather_desc, 'Clear') as weather_desc,
        
        to_timestamp(CAST(timestamp AS BIGINT)) as observed_at
    FROM {{ var('raw_database', 'skyeco_dev') }}.main.raw_flights 
    WHERE latitude IS NOT NULL 
      AND longitude IS NOT NULL
)
SELECT 
    *,
    (velocity_ms * 3.6) as speed_kmh,
    
    -- Refined Carbon Logic:
    -- Now safe from NULLs because of COALESCE above
    (
        (3.0 - (altitude_m * 0.0001)) -- Base burn that changes with height
        * (1 + (wind_speed_mps * 0.005)) -- Progressive wind penalty
        * (1 + (CASE WHEN air_temp_c < 15 THEN (15 - air_temp_c) * 0.002 ELSE 0 END)) -- Cold air penalty
        * 3.16 -- Carbon conversion factor
    )as co2_kg_per_km
FROM base_data
-- Filter out stationary 'flights' or data glitches
WHERE velocity_ms > 0