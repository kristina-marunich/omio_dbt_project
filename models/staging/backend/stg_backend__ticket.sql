-- stg_backend__ticket.sql

{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "uploaded_at_date", "data_type": "date"},
        cluster_by = ["booking_id"],
        on_schema_change = "fail"
    )
}}

with base as (
    select
        bookingid as booking_id,
        uploaded_at,
        json_extract_array(raw_json, '$.tickets') as tickets_array
    from {{ ref('base_backend__booking') }}
),

tickets as (
    select
        b.booking_id,
        b.uploaded_at,
        t as ticket_json
    from base b, unnest(b.tickets_array) as t
),

flattened as (
    select
        cast(booking_id as string) as booking_id,
        cast(json_value(ticket_json, '$.ticketid') as string) as ticket_id,
        safe_cast(json_value(ticket_json, '$.bookingPrice') as numeric) as ticket_price,
        cast(json_value(ticket_json, '$.bookingCurrency') as string) as ticket_currency,
        cast(json_value(ticket_json, '$.vendorCode') as string) as vendor_code,
        cast(uploaded_at as timestamp) as uploaded_at_timestamp,
        date(uploaded_at) as uploaded_at_date
    from tickets
)

{% if is_incremental() %}
select *
from flattened
where uploaded_at_date >= date_sub(current_date(), interval 7 day)
{% else %}
select * from flattened
{% endif %}
