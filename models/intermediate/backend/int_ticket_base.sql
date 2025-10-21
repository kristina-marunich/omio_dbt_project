-- models/intermediate/int_ticket_base.sql
with
    tickets_base as (
        select
            t.ticket_id as ticket_id,
            t.booking_id as booking_id,
            t.ticket_price,
            t.ticket_currency,

            date(b.created_at_timestamp) as transaction_date,

            cast(
                format_date('%Y%m%d', date(b.created_at_timestamp)) as int
            ) as ticket_issue_date_id

        from {{ ref("stg_backend__ticket") }} t
        inner join {{ ref("stg_backend__booking") }} b on t.booking_id = b.booking_id
    ),

    currency_rates as (
        select date, from_currency, rate from {{ ref("stg_finance__currencies") }}
    )

select
    tb.ticket_id, --PK
    tb.booking_id, -- FK
    tb.ticket_issue_date_id,

    tb.ticket_price,
    tb.ticket_currency,

    coalesce(r.rate, 1.0) as eur_rate,
    round(tb.ticket_price * coalesce(r.rate, 1.0), 2) as ticket_price_eur

from tickets_base tb
left join
    currency_rates r
    on upper(tb.ticket_currency) = upper(r.from_currency)
    and tb.transaction_date = r.date
