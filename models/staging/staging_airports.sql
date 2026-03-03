 WITH airports_regions_join AS (
        SELECT * 
        FROM {{source('flights_data', 'airports_raw')}}
        LEFT JOIN {{source('regions_data', 'regions')}}
        USING (country)
    )
    SELECT * FROM airports_regions_join