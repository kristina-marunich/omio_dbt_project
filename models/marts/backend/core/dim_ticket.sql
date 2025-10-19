-- dim_ticket.sql

SELECT
    ticket_id, -- PK 
    ticket_price,
    ticket_currency,
    vendor_code

FROM {{ ref('stg_backend__ticket') }}