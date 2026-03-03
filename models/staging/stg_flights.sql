
{{ config(materialized='table') }}
SELECT
    flight_date,
    origin,
    dest,
    cancelled,
    arr_delay,
    dep_delay
FROM clear_skies.flights_raw