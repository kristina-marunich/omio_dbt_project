-- stg_backend__segment.sql
-- Flatten array of segments within each booking

{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'segment_id'],
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        segment
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.segments')) as segment
    {% if is_incremental() %}
        where uploaded_at >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
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