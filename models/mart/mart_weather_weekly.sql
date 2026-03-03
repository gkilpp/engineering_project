WITH daily AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
)

SELECT
    airport_code,
    station_id,
    date_year                           AS year,
    cw                                  AS calendar_week,

    -- Temperature: avg/min/max make sense
    ROUND(AVG(avg_temp_c), 2)           AS avg_temp_c,
    MIN(min_temp_c)                     AS min_temp_c,
    MAX(max_temp_c)                     AS max_temp_c,

    -- Precipitation & snow: sum over the week
    SUM(precipitation_mm)               AS total_precipitation_mm,
    SUM(max_snow_mm)                    AS total_snow_mm,

    -- Wind direction: average (circular avg would be ideal but AVG is acceptable here)
    ROUND(AVG(avg_wind_direction), 0)   AS avg_wind_direction,

    -- Wind speed: average for typical conditions, max for peak
    ROUND(AVG(avg_wind_speed_kmh), 2)   AS avg_wind_speed_kmh,
    MAX(wind_peakgust_kmh)              AS max_wind_peakgust_kmh,

    -- Pressure: average
    ROUND(AVG(avg_pressure_hpa), 2)     AS avg_pressure_hpa,

    -- Sunshine: sum (total sunny minutes in the week)
    SUM(sun_minutes)                    AS total_sun_minutes,

    -- Season: take the mode (most frequent value in the week)
    -- Using a subquery approach compatible with PostgreSQL
    (
        SELECT season
        FROM daily d2
        WHERE d2.airport_code = daily.airport_code
          AND d2.date_year    = daily.date_year
          AND d2.cw           = daily.cw
        GROUP BY season
        ORDER BY COUNT(*) DESC
        LIMIT 1
    )                                   AS season,

    COUNT(*)                            AS days_with_data

FROM daily
GROUP BY airport_code, station_id, date_year, cw
ORDER BY airport_code, date_year, cw