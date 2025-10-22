
{{ config(materialized="table") }}

with
    booking_ticket_base as (
        -- 1. Calculate Ticket Counts and Revenue directly from the Fact table
        -- (Correct Grain)
        select
            f.booking_id,
            d.year_month_id,
            sum(f.ticket_price_eur) as total_booking_price,
            count(f.ticket_id) as total_tickets

        from {{ ref("fact_ticket_transaction") }} f
        inner join {{ ref("dim_date") }} d on f.ticket_issue_date_id = d.date_id
        group by 1, 2
    ),

    booking_passenger_base as (
        -- 2. Calculate unique MASTER passengers per booking (using the Bridge)
        select f.booking_id, count(distinct btp.passenger_id) as total_passengers

        from {{ ref("fact_ticket_transaction") }} f
        inner join
            {{ ref("bridge_ticket_passenger") }} btp on f.ticket_id = btp.ticket_id
        group by 1
    ),

    monthly_booking_base as (
        -- 3. Combine Ticket and Passenger metrics at the booking level
        select
            t.year_month_id,
            t.booking_id,
            t.total_booking_price,
            t.total_tickets,
            coalesce(p.total_passengers, 0) as total_passengers

        from booking_ticket_base t
        left join booking_passenger_base p on t.booking_id = p.booking_id
    ),

    booking_metrics as (
        -- 4. Calculate final monthly aggregates
        select
            year_month_id,
            count(distinct booking_id) as total_bookings,
            sum(total_booking_price) as total_revenue_eur,
            sum(total_tickets) as total_tickets,
            sum(total_passengers) as total_passengers,

            avg(total_tickets) as avg_tickets_per_booking,
            avg(total_passengers) as avg_passengers_per_booking,
            safe_divide(
                sum(total_booking_price), count(distinct booking_id)
            ) as avg_booking_value_eur,

            sum(total_booking_price) as total_ticket_revenue_eur,
            safe_divide(
                sum(total_booking_price), sum(total_tickets)
            ) as avg_ticket_price_eur

        from monthly_booking_base
        group by 1
    ),

    monthly_segment_metrics as (
        -- 5. Calculate unique segments (remains correct as it uses DISTINCT on the
        -- Bridge)
        select d.year_month_id, count(distinct bs.segment_id) as unique_segments
        from {{ ref("bridge_ticket_segment") }} bs
        inner join {{ ref("fact_ticket_transaction") }} f on bs.ticket_id = f.ticket_id
        inner join {{ ref("dim_date") }} d on f.ticket_issue_date_id = d.date_id
        group by 1
    ),

    monthly_passenger_metrics as (
        -- 6. Calculate total unique passengers in the month
        select d.year_month_id, count(distinct btp.passenger_id) as unique_passengers
        from {{ ref("bridge_ticket_passenger") }} btp
        inner join {{ ref("fact_ticket_transaction") }} f on btp.ticket_id = f.ticket_id
        inner join {{ ref("dim_date") }} d on f.ticket_issue_date_id = d.date_id
        group by 1
    ),

    ratios as (
        -- 7. Calculate ratios
        select
            b.year_month_id,
            safe_divide(
                b.total_bookings, p.unique_passengers
            ) as avg_bookings_per_passenger,
            safe_divide(s.unique_segments, b.total_bookings) as avg_segments_per_booking
        from booking_metrics b
        left join monthly_passenger_metrics p using (year_month_id)
        left join monthly_segment_metrics s using (year_month_id)
    ),

    monthly_passenger_type_metrics as (
        select
            d.year_month_id,
            dp.passenger_type,
            avg(f.ticket_price_eur) as avg_price_by_type
        from {{ ref("fact_ticket_transaction") }} f
        inner join
            {{ ref("bridge_ticket_passenger") }} btp on f.ticket_id = btp.ticket_id
        inner join {{ ref("dim_passenger") }} dp on btp.passenger_id = dp.passenger_id  -- Uses dim_passenger
        inner join {{ ref("dim_date") }} d on f.ticket_issue_date_id = d.date_id
        group by 1, 2
    ),

    -- You would need to PIVOT this data if you want it all on one row for the final
    -- agg table.
    -- For simplicity, we'll calculate the overall monthly average adult price as one
    -- metric.
    monthly_avg_adult_price as (
        select year_month_id, avg_price_by_type as avg_adult_ticket_price_eur
        from monthly_passenger_type_metrics
        where passenger_type = 'adult'
    -- Note: If multiple tickets are linked to one passenger, the AVG is inflated. 
    -- A true metric would average the unique booking's portion of the ticket price.
    )

select
    b.year_month_id,
    b.total_bookings,
    b.total_revenue_eur,
    b.total_tickets,
    b.total_passengers,
    b.avg_booking_value_eur,
    b.avg_tickets_per_booking,
    b.avg_passengers_per_booking,

    b.total_tickets as total_tickets_unique,
    b.avg_ticket_price_eur,
    b.total_ticket_revenue_eur,

    p.unique_passengers,
    s.unique_segments,

    r.avg_bookings_per_passenger,
    r.avg_segments_per_booking,
    aa.avg_adult_ticket_price_eur

from booking_metrics b
left join monthly_passenger_metrics p using (year_month_id)
left join monthly_segment_metrics s using (year_month_id)
left join ratios r using (year_month_id)
left join monthly_avg_adult_price aa using (year_month_id)
