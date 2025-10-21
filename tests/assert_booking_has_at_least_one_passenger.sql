select b.booking_id
from {{ ref("dim_booking") }} b
left join
    (
        -- Get the count of unique master passengers per booking
        select f.booking_id, count(distinct btp.passenger_id) as total_passengers
        from {{ ref("fact_ticket_transaction") }} f
        inner join
            {{ ref("bridge_ticket_passenger") }} btp on f.ticket_id = btp.ticket_id
        group by 1
    ) c
    on b.booking_id = c.booking_id

-- If the count is 0 or NULL, the test fails, flagging that booking_id
where coalesce(c.total_passengers, 0) = 0
