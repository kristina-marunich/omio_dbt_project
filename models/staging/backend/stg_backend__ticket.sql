-- stg_backend__ticket.sql
-- Flatten tickets array
with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        ticket
    from {{ ref('base_backend__booking') }},
         unnest(json_extract_array(raw_json, '$.tickets')) as ticket
)
select
    cast(booking_id as string) as booking_id,
    cast(json_value(ticket, '$.ticketid') as string) as ticket_id,
    safe_cast(json_value(ticket, '$.bookingPrice') as numeric) as ticket_price, -- we recieve this information as booking price but actually it is ticket price
    cast(json_value(ticket, '$.bookingCurrency') as string) as ticket_currency, -- the currency of payment
    cast(json_value(ticket, '$.vendorCode') as string) as vendor_code,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source
