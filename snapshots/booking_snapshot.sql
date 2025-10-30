{% snapshot booking_header_snapshot %}

    {{
        config(
            unique_key="booking_id",
            strategy="check",
            check_cols=["partner_id", "user_selected_currency"]
        )
    }}

    select
        booking_id,
        created_at_timestamp,
        updated_at_timestamp,
        user_selected_currency,
        partner_id

    from {{ ref("stg_backend__booking") }}

{% endsnapshot %}
