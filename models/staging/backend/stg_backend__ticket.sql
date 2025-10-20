-- stg_backend__ticket.sql
-- Flatten tickets array

{{ config(
    materialized='incremental',
    unique_key=['booking_id', 'ticket_id'],
    incremental_strategy='insert_overwrite',
    partition_by={'field': 'uploaded_at_timestamp', 'data_type': 'timestamp'},
    on_schema_change='sync'
) }}

with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        ticket
    from {{ ref('base_backend__booking') }}
        cross join unnest(json_extract_array(raw_json, '$.tickets')) as ticket
    {% if is_incremental() %}
        where uploaded_at >= timestamp_sub(current_timestamp(), interval 7 day)
    {% endif %}
)
select
    cast(booking_id as string) as booking_id,
    cast(json_value(ticket, '$.ticketid') as string) as ticket_id,
    safe_cast(json_value(ticket, '$.bookingPrice') as numeric) as ticket_price, -- we recieve this information as booking price but actually it is ticket price
    cast(json_value(ticket, '$.bookingCurrency') as string) as ticket_currency, -- the currency of payment
    cast(json_value(ticket, '$.vendorCode') as string) as vendor_code,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source
