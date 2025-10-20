-- int_ticket_summary.sql

{{ config(
    materialized = 'incremental',
    unique_key = 'ticket_id',
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change = 'sync'
) }}

-- Base ticket info
with tickets as (
    select
        booking_id,
        ticket_id,
        ticket_price,
        ticket_currency,
        vendor_code,
        uploaded_at_timestamp,
        -- include any other ticket-level fields you want
    from {{ ref('stg_backend__ticket') }}
    {% if is_incremental() %}
        where uploaded_at_timestamp >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
),

-- Count distinct segments per ticket
segments_count as (
    select
        ticket_id,
        count(distinct segment_id) as num_segments
    from {{ ref('stg_backend__ticket_segment') }}
    group by 1
),

-- Count distinct passengers per ticket
passengers_count as (
    select
        ticket_id,
        count(distinct passenger_id) as num_passengers
    from {{ ref('stg_backend__ticket_passenger') }}
    group by 1
) ,

-- currency rates (per day, per currency)
rates as (
    select
        date,
        from_currency,
        rate
    from {{ ref('stg_finance__currencies') }}
)

select
    t.ticket_id,
    t.booking_id,
    t.ticket_price,
    t.ticket_currency,
    t.vendor_code,
    coalesce(s.num_segments, 0) as num_segments,
    coalesce(p.num_passengers, 0) as num_passengers,
    coalesce(r.rate, 1.0) as eur_rate,
    round(t.ticket_price * coalesce(r.rate, 1.0), 2) as ticket_price_eur,
    t.uploaded_at_timestamp
from tickets t
left join segments_count s using (ticket_id)
left join passengers_count p using (ticket_id)
left join rates r
    on upper(t.ticket_currency) = upper(r.from_currency)
   and date(t.uploaded_at_timestamp) = r.date