-- models/intermediate/int_booking_measures.sql
-- Define mock exchange rates for demonstration. In production, this would be a JOIN
-- to a dim_exchange_rate table.
with
    currency_rates as (
        select 'EUR' as currency_code, 1.0 as rate_to_eur  -- Base Currency
        union all
        select 'USD', 0.95  -- Mock Rate: 1 USD = 0.95 EUR
        union all
        select 'JPY', 0.0065
    ),

    -- 1. Aggregation of Tickets (Primary Driver) - Now includes conversion
    tickets_agg as (
        select
            t.booking_id,
            sum(t.ticket_price) as total_booking_price_local,  -- Sum in original (mixed) currencies
            -- Convert price to EUR before summing: (price * rate_to_EUR)
            sum(t.ticket_price * r.rate_to_eur) as total_booking_price_eur,
            count(t.ticket_id) as num_tickets
        from {{ ref("stg_backend__ticket") }} as t
        left join currency_rates as r on t.ticket_currency = r.currency_code
        group by 1
    ),

    -- 2. Aggregation of Passengers
    passengers_agg as (
        select booking_id, count(passenger_id) as num_passengers
        from {{ ref("stg_backend__passenger") }}
        group by 1
    ),

    -- 3. Aggregation of Segments
    segments_agg as (
        select booking_id, count(segment_id) as num_segments
        from {{ ref("stg_backend__segment") }}
        group by 1
    )

-- Final SELECT: Join all aggregated measures
select
    -- Select the non-null booking_id from the joined tables (using COALESCE)
    coalesce(t.booking_id, p.booking_id, s.booking_id) as booking_id,

    -- Measures: The new standardized measure and the original measure
    coalesce(t.total_booking_price_local, 0) as total_booking_price_local,
    coalesce(t.total_booking_price_eur, 0) as total_booking_price_eur,

    -- Other measures
    coalesce(t.num_tickets, 0) as num_tickets,
    coalesce(p.num_passengers, 0) as num_passengers,
    coalesce(s.num_segments, 0) as num_segments

from tickets_agg as t
full outer join passengers_agg as p on t.booking_id = p.booking_id
full outer join segments_agg as s on coalesce(t.booking_id, p.booking_id) = s.booking_id
order by 1
