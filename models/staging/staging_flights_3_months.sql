{{ config(materialized='view') }}

WITH flights_three_months AS (
    SELECT *
FROM {{ source('flights_data', 'flights') }}
WHERE flight_date >= '2005-07-01'
  AND flight_date <  '2005-10-01'
    )
SELECT * FROM flights_three_months