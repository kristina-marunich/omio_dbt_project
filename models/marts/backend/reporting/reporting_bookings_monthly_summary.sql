-- models/marts/backend/reporting_bookings_monthly_summary.sql
-- Goal: Calculate monthly uniques and averages using only Core and Intermediate models.
-- models/marts/backend/reporting/rep_monthly_summary.sql
-- Goal: Calculate monthly uniques and averages using only Core and Intermediate models.
-- CTE to aggregate unique passengers and segments to the monthly level
-- This relies on the created_at timestamp being correctly included in the
-- Intermediate Link Tables.
with
    monthly_uniques as (
        select
            date_trunc(booking_created_at_timestamp, month) as reporting_month,  -- BigQuery uses DATE_TRUNC(date_expression, date_part)
            count(distinct passenger_id) as total_unique_passengers,
            null as total_unique_segments  -- Placeholder for UNION
        from {{ ref("int_ticket_passenger_links") }}
        group by 1

        union all

        select
            date_trunc(booking_created_at_timestamp, month) as reporting_month,
            null as total_unique_passengers,  -- Placeholder for UNION
            count(distinct segment_id) as total_unique_segments
        from {{ ref("int_ticket_segment_links") }}
        group by 1
    ),

    -- Pivot the unique counts from rows to columns
    pivoted_uniques as (
        select
            reporting_month,
            -- We now SUM the pivoted rows, grouping by month
            sum(total_unique_passengers) as total_unique_passengers,
            sum(total_unique_segments) as total_unique_segments
        from monthly_uniques
        group by 1
    ),

    -- Aggregate measures from the central fact table
    monthly_facts as (
        select
            date_trunc(created_at_timestamp, month) as reporting_month,
            count(booking_id) as total_bookings_made,
            sum(total_booking_price_eur) as total_revenue_eur,
            sum(num_segments) as total_segments_count,
            sum(num_passengers) as total_passengers_count
        from {{ ref("fact_booking") }}
        group by 1
    )

select
    f.reporting_month,

    -- Basic Aggregated Measures
    f.total_bookings_made,
    f.total_revenue_eur,

    -- Unique Entity Counts (from 'pivoted_uniques' CTE)
    u.total_unique_passengers,
    u.total_unique_segments,

    -- Derived Metrics (Ratios)
    safe_divide(
        f.total_bookings_made, u.total_unique_passengers
    ) as avg_bookings_per_unique_passenger,
    safe_divide(
        f.total_segments_count, f.total_bookings_made
    ) as avg_segments_per_booking_monthly,
    safe_divide(
        f.total_revenue_eur, f.total_bookings_made
    ) as average_booking_value_eur,
    safe_divide(
        f.total_passengers_count, f.total_bookings_made
    ) as average_passengers_per_booking

from monthly_facts as f
inner join pivoted_uniques as u on f.reporting_month = u.reporting_month
order by f.reporting_month
