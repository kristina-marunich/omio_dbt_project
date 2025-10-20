-- int_ticket_details.sql

with
tickets as (
    select
        booking_id,
        ticket_id,
        ticket_price,
        ticket_currency,
        vendor_code,
        uploaded_at_timestamp
    from {{ ref('stg_backend__ticket') }}
),

segments as (
    select
        segment_id,
        carrier_name,
        travel_mode,
        departure_time_timestamp,
        arrival_time_timestamp
    from {{ ref('stg_backend__segment') }}
),

passengers as (
    select
        passenger_id,
        passenger_type
    from {{ ref('stg_backend__passenger') }}
),

ticket_segments as (
    select
        booking_id,
        ticket_id,
        segment_id
    from {{ ref('stg_backend__ticket_segment') }}
),

ticket_passengers as (
    select
        booking_id,
        ticket_id,
        passenger_id
    from {{ ref('stg_backend__ticket_passenger') }}
)

select
    t.ticket_id,
    t.booking_id,
    ts.segment_id,
    tp.passenger_id,
    t.ticket_price,
    t.ticket_currency,
    t.vendor_code,
    s.carrier_name,
    s.travel_mode,
    s.departure_time_timestamp,
    s.arrival_time_timestamp,
    p.passenger_type,
    t.uploaded_at_timestamp
from tickets t
left join ticket_segments ts using (booking_id, ticket_id)
left join segments s on s.segment_id = ts.segment_id
left join ticket_passengers tp using (booking_id, ticket_id)
left join passengers p on p.passenger_id = tp.passenger_id