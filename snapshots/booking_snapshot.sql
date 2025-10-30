{% snapshot booking_header_snapshot %}

    {{
        config(
            unique_key="booking_id",
            strategy="check",
            updated_at="updated_at_timestamp",
            invalidate_hard_deletes=True,
            check_cols=["partner_id", "user_selected_currency"],
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
