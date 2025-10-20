{{ config(
    materialized = 'table'
) }}

-- Base booking-level info
with booking_base as (
    select
        date_trunc(date(created_at_timestamp), month) as month,
        booking_id,
        total_booking_price,
        total_tickets,
        total_passengers
    from {{ ref('int_booking_summary') }}
),

-- Booking-level monthly metrics
booking_metrics as (
    select
        month,
        count(distinct booking_id) as total_bookings,
        sum(total_booking_price) as total_revenue,
        sum(total_tickets) as total_tickets,
        sum(total_passengers) as total_passengers,
        avg(total_tickets) as avg_tickets_per_booking,
        avg(total_passengers) as avg_passengers_per_booking,
        safe_divide(sum(total_booking_price), count(distinct booking_id)) as avg_booking_value
    from booking_base
    group by month
),

-- Ticket-level metrics (converted to EUR)
ticket_metrics as (
    select
        date_trunc(date(t.uploaded_at_timestamp), month) as month,
        count(distinct t.ticket_id) as total_tickets_unique,
        avg(t.ticket_price_eur) as avg_ticket_price_eur,
        sum(t.ticket_price_eur) as total_revenue_eur
    from {{ ref('int_ticket_summary') }} t
    group by 1
),

-- Passenger-level metrics (distinct passengers per month)
passenger_metrics as (
    select
        date_trunc(date(b.created_at_timestamp), month) as month,
        count(distinct bp.passenger_id) as unique_passengers
    from {{ ref('stg_backend__booking') }} b
    join {{ ref('stg_backend__ticket') }} t using (booking_id)
    join {{ ref('stg_backend__ticket_passenger') }} bp using (booking_id, ticket_id)
    group by 1
),

-- Segment-level metrics (distinct segments per month)
segment_metrics as (
    select
        date_trunc(date(b.created_at_timestamp), month) as month,
        count(distinct ts.segment_id) as unique_segments
    from {{ ref('stg_backend__booking') }} b
    join {{ ref('stg_backend__ticket') }} t using (booking_id)
    join {{ ref('stg_backend__ticket_segment') }} ts using (booking_id, ticket_id)
    group by 1
),

-- Derived ratio-based KPIs
ratios as (
    select
        b.month,
        safe_divide(b.total_bookings, p.unique_passengers) as avg_bookings_per_passenger,
        safe_divide(s.unique_segments, b.total_bookings) as avg_segments_per_booking
    from booking_metrics b
    left join passenger_metrics p using (month)
    left join segment_metrics s using (month)
)

-- Final output
select
    b.month,
    b.total_bookings,
    b.total_revenue,
    b.total_tickets,
    b.total_passengers,
    b.avg_booking_value,
    b.avg_tickets_per_booking,
    b.avg_passengers_per_booking,
    t.total_tickets_unique,
    t.avg_ticket_price_eur,
    t.total_revenue_eur,
    p.unique_passengers,
    s.unique_segments,
    r.avg_bookings_per_passenger,
    r.avg_segments_per_booking
from booking_metrics b
left join ticket_metrics t using (month)
left join passenger_metrics p using (month)
left join segment_metrics s using (month)
left join ratios r using (month)
order by b.month
