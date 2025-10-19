-- int_ticket_passenger_links.sql
-- Resolves the M:N relationship between tickets and passengers, adding descriptive
-- context.
select
    tp.ticket_id,
    tp.passenger_id,
    t.booking_id,
    p.passenger_type,
    b.created_at_timestamp as booking_created_at_timestamp
from {{ ref("stg_backend__ticket_passenger") }} as tp  -- Raw join table
inner join
    {{ ref("stg_backend__ticket") }} as t  -- Join to Ticket to get bookingid
    on tp.ticket_id = t.ticket_id
inner join
    {{ ref("stg_backend__passenger") }} as p  -- Join to Passenger to get descriptive attributes
    on tp.passenger_id = p.passenger_id
inner join {{ ref("stg_backend__booking") }} as b on t.booking_id = b.booking_id
