-- stg_backend__segment.sql
-- Flatten array of segments within each booking

with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        segment
    from {{ ref('base_backend__booking') }},
         unnest(json_extract_array(raw_json, '$.segments')) as segment
)
select
    booking_id,
    cast(json_value(segment, '$.segmentid') as string) as segment_id,
    cast(json_value(segment, '$.carriername') as string) as carrier_name,
    cast(json_value(segment, '$.departuredatetime') as timestamp) as departure_time_timestamp,
    cast(json_value(segment, '$.arrivaldatetime') as timestamp) as arrival_time_timestamp,
    cast(json_value(segment, '$.travelmode') as string) as travel_mode,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source