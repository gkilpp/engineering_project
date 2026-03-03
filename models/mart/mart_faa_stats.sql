WITH flights AS (
    SELECT *
    FROM {{ ref('prep_flights') }}
),

airports AS (
    SELECT *
    FROM {{ ref('prep_airports') }}
),

departures AS (
    SELECT
        origin AS faa,
        COUNT(DISTINCT dest)                            AS unique_departure_connections,
        COUNT(*)                                        AS total_departures,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_departures,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)  AS diverted_departures,
        COUNT(DISTINCT tail_number)                     AS unique_airplanes_departing,
        COUNT(DISTINCT airline)                         AS unique_airlines_departing
    FROM flights
    GROUP BY origin
),

arrivals AS (
    SELECT
        dest AS faa,
        COUNT(DISTINCT origin)                          AS unique_arrival_connections,
        COUNT(*)                                        AS total_arrivals,
        SUM(CASE WHEN cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_arrivals,
        SUM(CASE WHEN diverted = 1 THEN 1 ELSE 0 END)  AS diverted_arrivals,
        COUNT(DISTINCT tail_number)                     AS unique_airplanes_arriving,
        COUNT(DISTINCT airline)                         AS unique_airlines_arriving
    FROM flights
    GROUP BY dest
),

combined AS (
    SELECT
        COALESCE(d.faa, a.faa)                                                          AS faa,
        COALESCE(d.unique_departure_connections, 0)                                     AS unique_departure_connections,
        COALESCE(a.unique_arrival_connections, 0)                                       AS unique_arrival_connections,
        COALESCE(d.total_departures, 0) + COALESCE(a.total_arrivals, 0)                AS total_flights,
        COALESCE(d.cancelled_departures, 0) + COALESCE(a.cancelled_arrivals, 0)        AS total_cancelled,
        COALESCE(d.diverted_departures, 0) + COALESCE(a.diverted_arrivals, 0)          AS total_diverted,
        (COALESCE(d.total_departures, 0) + COALESCE(a.total_arrivals, 0))
            - (COALESCE(d.cancelled_departures, 0) + COALESCE(a.cancelled_arrivals, 0))
            - (COALESCE(d.diverted_departures, 0) + COALESCE(a.diverted_arrivals, 0)) AS total_actual_flights,
        COALESCE(d.unique_airplanes_departing, 0) + COALESCE(a.unique_airplanes_arriving, 0) AS unique_airplanes,
        COALESCE(d.unique_airlines_departing, 0) + COALESCE(a.unique_airlines_arriving, 0)   AS unique_airlines
    FROM departures d
    FULL OUTER JOIN arrivals a ON d.faa = a.faa
)

SELECT
    c.*,
    ap.name     AS airport_name,
    ap.city,
    ap.country
FROM combined c
LEFT JOIN airports ap ON c.faa = ap.faa
ORDER BY total_flights DESC
