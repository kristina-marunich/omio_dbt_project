-- fact_booking.sql
-- The grain is one row per Booking.

SELECT
    -- 1. Descriptive and Time Attributes (from Staging)
    b.booking_id, -- Primary Key for the Fact table
    b.created_at_timestamp,
    b.updated_at_timestamp,
    b.user_selected_currency,
    b.partner_id,

    -- 2. Foreign Keys (FKs)
    -- NOTE: In a transaction Fact table, you typically link the main entity (Booking)
    -- to Dimensions. For the M:N dimensions (Ticket/Passenger/Segment), you will need
    -- a link to the join/link tables for drill-down, but for the main analysis,
    -- the Booking is the primary entity.

    -- For simplicity, we can link to the dimension that shares the lowest grain,
    -- but here we link to the Booking's components.
    -- (No direct FK to dim_passenger/dim_segment/dim_ticket is necessary as they are M:N
    -- and accessed via the Booking or Intermediate links)

    -- 3. Measures (from Intermediate)
    m.total_booking_price_local,
    m.total_booking_price_eur,
    m.num_tickets,
    m.num_passengers,
    m.num_segments

FROM {{ ref('stg_backend__booking') }} AS b
INNER JOIN {{ ref('int_booking_measures') }} AS m
    ON b.booking_id = m.booking_id
