-- int_booking_ticket_segment_passenger.sql
-- Combine ticket, segment, and passenger details into a unified record

{{ config(
    materialized = 'incremental',
    unique_key = ['booking_id', 'ticket_id', 'segment_id', 'passenger_id'],
    incremental_strategy = 'insert_overwrite',
    partition_by = {'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change = 'sync'
) }}

with tickets as (
    select
        booking_id,
        ticket_id,
        uploaded_at_timestamp
    from {{ ref('stg_backend__ticket') }}
    {% if is_incremental() %}
        where uploaded_at_timestamp >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
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
    t.booking_id,
    t.ticket_id,
    ts.segment_id,
    tp.passenger_id,
    t.uploaded_at_timestamp
from tickets t
left join ticket_segments ts using (booking_id, ticket_id)
left join ticket_passengers tp using (booking_id, ticket_id)