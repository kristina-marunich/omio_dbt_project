-- models/marts/core/bridge_ticket_passenger.sql


SELECT
    ticket_id, -- FK: Ticket ID
    passenger_id -- FK: Master Passenger ID
FROM {{ ref('stg_backend__ticket_passenger') }}

-- Ensure all keys exist in the master dimension
INNER JOIN {{ ref('dim_passenger') }} p
    USING (passenger_id)