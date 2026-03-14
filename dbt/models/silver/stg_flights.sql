-- models/silver/stg_flights.sql
{{ config(materialized='table') }}

WITH base_data AS (
    SELECT 
        icao24,
        callsign,
        origin_country,
        latitude,
        longitude,
        -- No need for complex JSON parsing here as the Python bridge 
        -- already inserted these as proper columns!
        CAST(altitude AS FLOAT) as altitude_m,
        CAST(velocity AS FLOAT) as velocity_ms,
        to_timestamp(CAST(timestamp AS BIGINT)) as observed_at
    FROM {{ var('raw_database', 'skyeco_dev') }}.main.raw_flights 
    WHERE latitude IS NOT NULL 
      AND longitude IS NOT NULL
)
SELECT 
    *,
    (velocity_ms * 3.6) as speed_kmh,
    -- Fuel burn logic: +20% if at low altitude (thicker air)
    CASE 
        WHEN altitude_m < 5000 THEN 2.8 
        ELSE 2.2 
    END * 3.16 as co2_kg_per_km
FROM base_data
WHERE velocity_ms > 0