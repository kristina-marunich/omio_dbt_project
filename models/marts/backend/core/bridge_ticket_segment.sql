-- models/marts/core/bridge_ticket_segment.sql


SELECT
    ticket_id, -- FK: Ticket ID
    segment_id -- FK: Master Segment ID
FROM {{ ref('stg_backend__ticket_segment') }}