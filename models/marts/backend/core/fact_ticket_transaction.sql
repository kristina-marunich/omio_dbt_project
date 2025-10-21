-- models/marts/core/fact_ticket_transaction.sql


SELECT
    ticket_id, -- PK: Ticket ID
    booking_id, -- FK: Booking ID
    ticket_issue_date_id, -- FK: Date ID
    
    -- Measures
    ticket_price_eur,
    ticket_price AS ticket_price_original,
    eur_rate,
    
    -- Contextual Dimensions
    ticket_currency

FROM {{ ref('int_ticket_base') }}