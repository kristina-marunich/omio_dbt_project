-- models/intermediate/int_ticket_base.sql

{{
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {"field": "uploaded_at_date", "data_type": "date"},
        cluster_by = ["booking_id"],
        on_schema_change = "fail"
    )
}}

with tickets as (
    select *
    from {{ ref("stg_backend__ticket") }}
    {% if is_incremental() %}
    where uploaded_at_date >= date_sub(current_date(), interval 7 day)
    {% endif %}
),
bookings as (
    select *
    from {{ ref("stg_backend__booking") }}
    {% if is_incremental() %}
    where uploaded_at_date >= date_sub(current_date(), interval 7 day)
    {% endif %}
),
currency_rates as (
    select date, from_currency, rate
    from {{ ref("stg_finance__currencies") }}
)

select
    t.ticket_id,
    t.booking_id,
    cast(format_date('%Y%m%d', date(b.created_at_timestamp)) as int) as ticket_issue_date_id,
    t.uploaded_at_timestamp,
    t.uploaded_at_date,
    t.ticket_price,
    t.ticket_currency,
    coalesce(r.rate, 1.0) as eur_rate,
    round(t.ticket_price * coalesce(r.rate, 1.0), 2) as ticket_price_eur
from tickets t
inner join bookings b on t.booking_id = b.booking_id
left join currency_rates r
    on upper(t.ticket_currency) = upper(r.from_currency)
   and date(b.created_at_timestamp) = r.date
