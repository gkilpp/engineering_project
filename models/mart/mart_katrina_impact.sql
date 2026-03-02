{{ config(materialized='table') }}

SELECT
    day,
    airport,
    flights,
    cancel_percentage,
    avg_wind_speed,
    avg_pressure,
    
    CASE
        WHEN avg_wind_speed > 20 THEN 'High Wind'
        WHEN avg_wind_speed BETWEEN 10 AND 20 THEN 'Moderate Wind'
        ELSE 'Low Wind'
    END AS wind_category

FROM {{ ref('prep_daily_flights_weather') }}