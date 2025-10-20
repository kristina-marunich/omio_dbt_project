-- dim_segment.sql

SELECT
    distinct
    segment_id, -- PK 
    carrier_name ,
    departure_time_timestamp,
    arrival_time_timestamp,
    travel_mode

FROM {{ ref('stg_backend__segment') }} s 
