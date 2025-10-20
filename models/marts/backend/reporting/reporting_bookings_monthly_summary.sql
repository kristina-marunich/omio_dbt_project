with booking_base as (
    select
        date_trunc(date(created_at_timestamp), month) as month,
        booking_id,
        total_booking_price, 
        total_tickets,
        total_passengers
    from {{ ref('fact_booking') }}
),

booking_metrics as (
    select
        month,
        count(distinct booking_id) as total_bookings,
        sum(total_booking_price) as total_revenue_eur,
        sum(total_tickets) as total_tickets,
        sum(total_passengers) as total_passengers,
        avg(total_tickets) as avg_tickets_per_booking,
        avg(total_passengers) as avg_passengers_per_booking,
        safe_divide(sum(total_booking_price), count(distinct booking_id)) as avg_booking_value_eur
    from booking_base
    group by month
),

ticket_metrics as (
    select
        date_trunc(date(uploaded_at_timestamp), month) as month,
        count(distinct ticket_id) as total_tickets_unique,
        avg(ticket_price_eur) as avg_ticket_price_eur,
        sum(ticket_price_eur) as total_ticket_revenue_eur
    from {{ ref('fact_ticket') }}
    group by 1
),

passenger_metrics as (
    select
        date_trunc(date(uploaded_at_timestamp), month) as month,
        count(distinct passenger_id) as unique_passengers
    from {{ ref('int_booking_ticket_segment_passenger') }}
    where passenger_id is not null
    group by 1
),


segment_metrics as (
    select
        date_trunc(date(uploaded_at_timestamp), month) as month,
        count(distinct segment_id) as unique_segments
    from {{ ref('int_booking_ticket_segment_passenger') }}
    where segment_id is not null
    group by 1
),

ratios as (
    select
        b.month,
        safe_divide(b.total_bookings, p.unique_passengers) as avg_bookings_per_passenger,
        safe_divide(s.unique_segments, b.total_bookings) as avg_segments_per_booking
    from booking_metrics b
    left join passenger_metrics p using (month)
    left join segment_metrics s using (month)
)

select
    b.month,
    b.total_bookings,
    b.total_revenue_eur,
    b.total_tickets,
    b.total_passengers,
    b.avg_booking_value_eur,
    b.avg_tickets_per_booking,
    b.avg_passengers_per_booking,
    t.total_tickets_unique,
    t.avg_ticket_price_eur,
    t.total_ticket_revenue_eur,
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
