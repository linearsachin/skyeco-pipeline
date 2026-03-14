-- Gold Layer: Aggregate Analytics
{{ config(materialized='table') }}

WITH silver_data AS (
    SELECT * FROM {{ ref('stg_flights') }}
)

SELECT 
    callsign,
    origin_country,
    COUNT(*) as signal_count,
    ROUND(AVG(speed_kmh), 2) as avg_speed,
    ROUND(AVG(co2_kg_per_km), 2) as avg_co2_intensity,
    -- Identify if the flight is high-impact
    CASE 
        WHEN AVG(co2_kg_per_km) > 10 THEN 'High Impact'
        ELSE 'Standard'
    END as environmental_rating
FROM silver_data
GROUP BY 1, 2
HAVING signal_count > 1  -- Filter out noise
ORDER BY avg_co2_intensity DESC