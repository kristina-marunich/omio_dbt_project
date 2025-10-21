-- stg_backend__booking.sql
-- Flatten top-level booking data


with source as (
    select
        bookingid as booking_id,
        uploaded_at,
        json_value(raw_json, '$.createdAt') as created_at,
        json_value(raw_json, '$.updatedAt') as updated_at,
        json_value(raw_json, '$.userSelectedCurrency') as user_selected_currency,
        json_value(raw_json, '$.partnerIdOffer') as partner_id
    from {{ ref('base_backend__booking') }}


)
select
    cast(booking_id as string) as booking_id,
    safe_cast(created_at as timestamp) as created_at_timestamp,
    safe_cast(updated_at as timestamp) as updated_at_timestamp,
    cast(user_selected_currency as string) as user_selected_currency,
    cast(partner_id as string) as partner_id,
    cast(uploaded_at as timestamp) as uploaded_at_timestamp
from source
