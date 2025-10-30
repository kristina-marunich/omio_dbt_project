-- models/marts/core/fact_ticket_transaction.sql
{{ 
    config(
        materialized = "incremental",
        incremental_strategy = "insert_overwrite",
        partition_by = {
            "field": "uploaded_at_date",  
            "data_type": "date"
        },
        cluster_by = ["booking_id"],      
        on_schema_change = "fail"
    )
}}

with source as (
    select
        ticket_id,  -- PK
        booking_id,  -- FK
        date(uploaded_at_timestamp) as uploaded_at_date,
        ticket_issue_date_id,
        ticket_price_eur,
        ticket_price as ticket_price_original,
        eur_rate,
        ticket_currency,
        uploaded_at_timestamp
    from {{ ref("int_ticket_base") }}
)

{% if is_incremental() %}
    -- Only rebuild the partitions that changed (e.g., last 7 days)
    select *
    from source
    where uploaded_at_date >= date_sub(current_date(), interval 7 day)
{% else %}
    select *
    from source
{% endif %}
