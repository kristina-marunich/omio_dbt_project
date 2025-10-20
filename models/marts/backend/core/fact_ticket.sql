-- models/marts/backend/core/fct_ticket.sql

select
    ticket_id,
    booking_id,
    ticket_price,
    ticket_currency,
    vendor_code,
    num_segments,
    num_passengers,
    eur_rate,
    ticket_price_eur,
    uploaded_at_timestamp
from {{ ref('int_ticket_summary') }}