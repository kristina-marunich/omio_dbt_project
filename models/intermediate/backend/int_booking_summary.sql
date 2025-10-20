-- models/intermediate/backend/int_booking_summary.sql
-- Aggregate booking-level metrics from tickets and passengers

{{ config(
    materialized='incremental',
    unique_key='booking_id',
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

with
bookings as (
    select
        booking_id,
        created_at_timestamp,
        updated_at_timestamp,
        user_selected_currency,
        partner_id,
        uploaded_at_timestamp
    from {{ ref('stg_backend__booking') }}
    {% if is_incremental() %}
        where uploaded_at_timestamp >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
),

tickets_agg as (
    select
        booking_id,
        sum(ticket_price) as total_booking_price,
        count(distinct ticket_id) as total_tickets
    from {{ ref('stg_backend__ticket') }}
    group by booking_id
),

passengers_agg as (
    select
        booking_id,
        count(distinct passenger_id) as total_passengers
    from {{ ref('stg_backend__passenger') }}
    group by booking_id
)

select
    b.booking_id,
    b.created_at_timestamp,
    b.updated_at_timestamp,
    b.user_selected_currency,
    b.partner_id,
    t.total_booking_price,
    t.total_tickets,
    p.total_passengers,
    b.uploaded_at_timestamp
from bookings b
left join tickets_agg t using (booking_id)
left join passengers_agg p using (booking_id)