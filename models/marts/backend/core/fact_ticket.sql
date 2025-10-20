-- models/marts/backend/core/fct_ticket.sql

select
    ticket_id,
    booking_id,
    segment_id,
    passenger_id,
    ticket_price,
    ticket_currency,
    vendor_code,
    carrier_name,
    travel_mode,
    departure_time_timestamp,
    arrival_time_timestamp,
    passenger_type,
    uploaded_at_timestamp
from {{ ref('int_ticket_details') }}