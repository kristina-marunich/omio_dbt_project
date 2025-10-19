-- int_ticket_segment_links.sql

-- int_ticket_segment_links.sql

-- Resolves the M:N relationship between tickets and segments, adding descriptive context.

SELECT
    ts.ticket_id,
    ts.segment_id,
    t.booking_id,
    s.carrier_name,
    s.travel_mode,
    s.departure_time_timestamp,
    b.created_at_timestamp as booking_created_at_timestamp
FROM {{ ref('stg_backend__ticket_segment') }} AS ts -- Raw join table
INNER JOIN {{ ref('stg_backend__ticket') }} AS t -- Join to Ticket to get bookingid
    ON ts.ticket_id = t.ticket_id
INNER JOIN {{ ref('stg_backend__segment') }} AS s -- Join to Segment to get descriptive attributes
    ON ts.segment_id = s.segment_id
inner join {{ ref("stg_backend__booking") }} as b on t.booking_id = b.booking_id