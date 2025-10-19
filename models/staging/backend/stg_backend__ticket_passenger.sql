-- stg_backend__ticket_passenger.sql
-- Flatten nested passengers within each ticket
with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        json_value(ticket, '$.ticketid') as ticket_id,
        passenger
    from {{ ref('base_backend__booking') }},
         unnest(json_extract_array(raw_json, '$.tickets')) as ticket,
         unnest(json_extract_array(ticket, '$.passengers')) as passenger
)
select
    booking_id,
    cast(ticket_id as string) as ticket_id,
    cast(json_value(passenger, '$.passengerId') as string) as passenger_id,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source