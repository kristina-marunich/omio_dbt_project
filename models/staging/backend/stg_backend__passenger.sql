-- stg_backend__passenger.sql
-- Flatten passengers array


with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        passenger
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.passengers')) as passenger
)
select
    cast(booking_id as string) as booking_id,
    cast(json_value(passenger, '$.passengerId') as string) as passenger_id,
    cast(json_value(passenger, '$.type') as string) as passenger_type,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source