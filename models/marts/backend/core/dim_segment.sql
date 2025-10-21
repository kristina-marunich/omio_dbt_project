-- dim_segment.sql

SELECT
    segment_id, -- PK: Master Segment ID
    carrier_name,
    travel_mode,
    earliest_departure_time
FROM {{ ref('int_segment_master_attributes') }}
