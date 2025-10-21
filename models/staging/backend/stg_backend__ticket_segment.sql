-- stg_backend__ticket_segment.sql
-- Flatten nested segments within each ticket


with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        json_value(ticket, '$.ticketid') as ticket_id,
        segment
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.tickets')) as ticket
        cross join unnest(json_extract_array(ticket, '$.segments')) as segment
)
select
    cast(booking_id as string) as booking_id,
    cast(ticket_id as string) as ticket_id,
    cast(json_value(segment, '$.segmentid') as string) as segment_id,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source