-- models/intermediate/int_segment_master_attributes.sql


SELECT
    segment_id, -- PK: Master ID for the Route/Product
    MAX(carrier_name) AS carrier_name,
    MAX(travel_mode) AS travel_mode,
    MIN(departure_time_timestamp) AS earliest_departure_time
FROM {{ ref('stg_backend__segment') }}
GROUP BY 1
