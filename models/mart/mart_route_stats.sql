WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

airports AS (
    SELECT *
    FROM {{ ref('prep_airports') }}
),

route_stats AS (
    SELECT
        origin,
        dest,
        COUNT(*)                                            AS total_flights,
        COUNT(DISTINCT tail_number)                         AS unique_airplanes,
        COUNT(DISTINCT airline)                             AS unique_airlines,
        ROUND(AVG(actual_elapsed_time), 2)                 AS avg_actual_elapsed_time_min,
        ROUND(AVG(arr_delay), 2)                           AS avg_arrival_delay_min,
        MAX(arr_delay)                                     AS max_arrival_delay_min,
        MIN(arr_delay)                                     AS min_arrival_delay_min,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END)    AS total_cancelled,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)     AS total_diverted
    FROM flights
    GROUP BY origin, dest
)

SELECT
    rs.origin,
    orig.name       AS origin_airport_name,
    orig.city       AS origin_city,
    orig.country    AS origin_country,
    rs.dest,
    dest.name       AS dest_airport_name,
    dest.city       AS dest_city,
    dest.country    AS dest_country,
    rs.total_flights,
    rs.unique_airplanes,
    rs.unique_airlines,
    rs.avg_actual_elapsed_time_min,
    rs.avg_arrival_delay_min,
    rs.max_arrival_delay_min,
    rs.min_arrival_delay_min,
    rs.total_cancelled,
    rs.total_diverted
FROM route_stats rs
LEFT JOIN airports orig ON rs.origin = orig.faa
LEFT JOIN airports dest ON rs.dest   = dest.faa
ORDER BY rs.total_flights DESC