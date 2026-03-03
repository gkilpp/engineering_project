WITH hourly_raw AS (
        SELECT

                airport_faa as airport_code,
                station as station_id
        FROM {{source('weather_data', 'weather_hourly_katrina_raw')}}
    ),
    hourly_data AS (
        SELECT  
                airport_code
                ,station_id
                ,'time'::TIMESTAMP AS timestamp	
                ,temp::NUMERIC AS temp_c
                ,dwpt::NUMERIC AS dewpoint_c
                ,rhum::NUMERIC AS humidity_perc
                ,prcp::NUMERIC AS precipitation_mm
                ,snow::INTEGER AS snow_mm
                ,wdir::NUMERIC::INTEGER AS wind_direction
                ,wspd::NUMERIC AS wind_speed_kmh
                ,wpgt::NUMERIC AS wind_peakgust_kmh
                ,pres::NUMERIC AS pressure_hpa 
                ,tsun::INTEGER AS sun_minutes
                ,coco::INTEGER AS condition_code
        FROM hourly_raw
    )
    SELECT * 
    FROM hourly_data