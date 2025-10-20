

select
    booking_id,
    created_at_timestamp as created_at,
    updated_at_timestamp as updated_at,
    partner_id,
    user_selected_currency as booking_currency,
    total_booking_price,
    total_tickets,
    total_passengers,
    uploaded_at_timestamp
from {{ ref('int_booking_summary') }}
