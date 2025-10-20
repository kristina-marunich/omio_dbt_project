-- stg_backend__passenger.sql
-- Flatten passengers array

{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'passenger_id'],   
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        passenger
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.passengers')) as passenger
    {% if is_incremental() %}
        where uploaded_at >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
)
select
    cast(booking_id as string) as booking_id,
    cast(json_value(passenger, '$.passengerId') as string) as passenger_id,
    cast(json_value(passenger, '$.type') as string) as passenger_type,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source