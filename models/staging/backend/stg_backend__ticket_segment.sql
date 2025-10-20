-- stg_backend__ticket_segment.sql
-- Flatten nested segments within each ticket

{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'ticket_id', 'segment_id'],
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        json_value(ticket, '$.ticketid') as ticket_id,
        segment
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.tickets')) as ticket
        cross join unnest(json_extract_array(ticket, '$.segments')) as segment
    {% if is_incremental() %}
        where uploaded_at >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
)
select
    cast(booking_id as string) as booking_id,
    cast(ticket_id as string) as ticket_id,
    cast(json_value(segment, '$.segmentid') as string) as segment_id,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source