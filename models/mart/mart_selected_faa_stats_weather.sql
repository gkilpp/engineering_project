-- Only airports we have weather data for, stats per day
WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

weather AS (
    SELECT *
    FROM {{ ref('prep_weather_daily') }}
),

airports AS (
    SELECT *
    FROM {{ ref('prep_airports') }}
),

-- airports that have weather data
weather_airports AS (
    SELECT DISTINCT airport_code
    FROM weather
),

departures_daily AS (
    SELECT
        origin                                              AS faa,
        flight_date,
        COUNT(DISTINCT dest)                                AS unique_departure_connections,
        COUNT(*)                                            AS total_departures,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)    AS cancelled_departures,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)     AS diverted_departures,
        COUNT(DISTINCT tail_number)                         AS unique_airplanes_departing,
        COUNT(DISTINCT airline)                             AS unique_airlines_departing
    FROM flights
    WHERE origin IN (SELECT airport_code FROM weather_airports)
    GROUP BY origin, flight_date
),

arrivals_daily AS (
    SELECT
        dest                                                AS faa,
        flight_date,
        COUNT(DISTINCT origin)                              AS unique_arrival_connections,
        COUNT(*)                                            AS total_arrivals,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)    AS cancelled_arrivals,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)     AS diverted_arrivals,
        COUNT(DISTINCT tail_number)                         AS unique_airplanes_arriving,
        COUNT(DISTINCT airline)                             AS unique_airlines_arriving
    FROM flights
    WHERE dest IN (SELECT airport_code FROM weather_airports)
    GROUP BY dest, flight_date
),

combined_daily AS (
    SELECT
        COALESCE(d.faa, a.faa)                                                              AS faa,
        COALESCE(d.flight_date, a.flight_date)                                              AS flight_date,
        COALESCE(d.unique_departure_connections, 0)                                         AS unique_departure_connections,
        COALESCE(a.unique_arrival_connections, 0)                                           AS unique_arrival_connections,
        COALESCE(d.total_departures, 0) + COALESCE(a.total_arrivals, 0)                    AS total_flights,
        COALESCE(d.cancelled_departures, 0) + COALESCE(a.cancelled_arrivals, 0)            AS total_cancelled,
        COALESCE(d.diverted_departures, 0) + COALESCE(a.diverted_arrivals, 0)              AS total_diverted,
        (COALESCE(d.total_departures, 0) + COALESCE(a.total_arrivals, 0))
            - (COALESCE(d.cancelled_departures, 0) + COALESCE(a.cancelled_arrivals, 0))
            - (COALESCE(d.diverted_departures, 0) + COALESCE(a.diverted_arrivals, 0))      AS total_actual_flights,
        COALESCE(d.unique_airplanes_departing, 0) + COALESCE(a.unique_airplanes_arriving, 0) AS unique_airplanes,
        COALESCE(d.unique_airlines_departing, 0) + COALESCE(a.unique_airlines_arriving, 0)   AS unique_airlines
    FROM departures_daily d
    FULL OUTER JOIN arrivals_daily a
        ON d.faa = a.faa AND d.flight_date = a.flight_date
)

SELECT
    c.faa,
    ap.name                 AS airport_name,
    ap.city,
    ap.country,
    c.flight_date,
    c.unique_departure_connections,
    c.unique_arrival_connections,
    c.total_flights,
    c.total_cancelled,
    c.total_diverted,
    c.total_actual_flights,
    c.unique_airplanes,
    c.unique_airlines,
    w.min_temp_c,
    w.max_temp_c,
    w.precipitation_mm,
    w.max_snow_mm,
    w.avg_wind_direction,
    w.avg_wind_speed_kmh,
    w.wind_peakgust_kmh
FROM combined_daily c
LEFT JOIN weather w
    ON c.faa = w.airport_code AND c.flight_date = w.date
LEFT JOIN airports ap
    ON c.faa = ap.faa
ORDER BY c.faa, c.flight_date
