-- models/marts/core/fact_ticket_transaction.sql
{{
    config(
        materialized="incremental",
        unique_key="ticket_id",
        incremental_strategy="merge",
        on_schema_change="fail",
    )
}}


select
    ticket_id,  -- PK: Ticket ID
    booking_id,  -- FK: Booking ID
    ticket_issue_date_id,  -- FK: Date ID

    -- Measures
    ticket_price_eur,
    ticket_price as ticket_price_original,
    eur_rate,

    -- Contextual Dimensions
    ticket_currency,
    uploaded_at_timestamp

from {{ ref("int_ticket_base") }}

{% if is_incremental() %}

    where
        uploaded_at_timestamp >= (
            select timestamp_sub(max(uploaded_at_timestamp), interval 7 day)
            from {{ this }}
        )

{% endif %}
