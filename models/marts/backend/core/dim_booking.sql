-- models/marts/core/dim_booking.sql

SELECT
    booking_id, -- PK: Booking ID
    user_selected_currency,
    partner_id,
    -- FK to dim_date for booking creation date
    CAST(FORMAT_DATE('%Y%m%d', DATE(created_at_timestamp)) AS INT) AS booking_created_date_id -- FK: Date ID
FROM {{ ref('stg_backend__booking') }}