{{ config(materialized='table') }}

SELECT
    time,
    airport_faa,
    wspd AS wind_speed,
    pres AS pressure
FROM clear_skies.weather_hourly_katrina_raw