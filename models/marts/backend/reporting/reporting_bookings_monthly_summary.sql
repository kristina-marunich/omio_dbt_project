-- rpt_monthly_booking_activity.sql
-- Purpose: Monthly aggregated KPIs from core fact tables (bookings, tickets, passengers, segments).

with
-- 1️⃣ Base: Define month from booking creation date
booking_base as (
    select
        date_trunc(date(created_at), month) as month,
        booking_id,
        total_booking_price,
        total_tickets,
        total_passengers
    from {{ ref('fact_booking') }}
),

-- 2️⃣ Aggregate booking-level metrics
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

-- 3️⃣ Aggregate ticket-level metrics
-- 3️⃣ Aggregate ticket-level metrics
ticket_metrics as (
    select
        date_trunc(date(b.created_at), month) as month,
        count(distinct t.ticket_id) as total_tickets_unique,
        avg(t.ticket_price) as avg_ticket_price
    from {{ ref('fact_ticket') }} t
    join {{ ref('fact_booking') }} b using (booking_id)
    group by 1
),

-- 4️⃣ Distinct passengers per month
passenger_metrics as (
    select
        date_trunc(date(b.created_at), month) as month,
        count(distinct p.passenger_id) as unique_passengers
    from {{ ref('fact_booking') }} b
    join {{ ref('fact_ticket') }} t using (booking_id)
    join {{ ref('dim_passenger') }} p using (passenger_id)
    group by 1
),

-- 5️⃣ Distinct segments per month
segment_metrics as (
    select
        date_trunc(date(b.created_at), month) as month,
        count(distinct s.segment_id) as unique_segments
    from {{ ref('fact_booking') }} b
    join {{ ref('fact_ticket') }} t using (booking_id)
    join {{ ref('dim_segment') }} s using (segment_id)
    group by 1
),

-- 6️⃣ Derived metrics based on ratios
ratios as (
    select
        b.month,
        safe_divide(b.total_bookings, p.unique_passengers) as avg_bookings_per_passenger,
        safe_divide(s.unique_segments, b.total_bookings) as avg_segments_per_booking
    from booking_metrics b
    left join passenger_metrics p using (month)
    left join segment_metrics s using (month)
)

-- 7️⃣ Final combined output
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
    t.avg_ticket_price,
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
