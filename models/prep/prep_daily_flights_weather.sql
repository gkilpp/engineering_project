{{ config(materialized='table') }}

WITH flights_daily AS (

    SELECT
        flight_date AS day,
        airport_code AS airport,
        COUNT(*) AS flights,
        AVG(cancelled::int) AS cancel_rate
    FROM {{ ref('stg_flights') }}
    GROUP BY 1,2

),

weather_daily AS (

    SELECT
        DATE(time) AS day,
        airport_faa AS airport,
        AVG(wind_speed) AS avg_wind_speed,
        AVG(pressure) AS avg_pressure
    FROM {{ ref('stg_weather_hourly') }}
    GROUP BY 1,2

)

SELECT
    f.day,
    f.airport,
    f.flights,
    f.cancel_rate,
    f.cancel_rate * 100 AS cancel_percentage,
    w.avg_wind_speed,
    w.avg_pressure
FROM flights_daily f
LEFT JOIN weather_daily w
    ON f.day = w.day
    AND f.airport = w.airport