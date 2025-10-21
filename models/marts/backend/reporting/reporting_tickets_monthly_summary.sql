-- models/marts/reporting/agg_monthly_executive_summary_replicated.sql
{{ config(materialized='table') }}

WITH booking_ticket_base AS (
    -- 1. Calculate Ticket Counts and Revenue directly from the Fact table (Correct Grain)
    SELECT
        f.booking_id,
        d.year_month_id,
        SUM(f.ticket_price_eur) AS total_booking_price, 
        COUNT(f.ticket_id) AS total_tickets
        
    FROM {{ ref('fact_ticket_transaction') }} f
    INNER JOIN {{ ref('dim_date') }} d ON f.ticket_issue_date_id = d.date_id
    GROUP BY 1, 2
),

booking_passenger_base AS (
    -- 2. Calculate unique MASTER passengers per booking (using the Bridge)
    SELECT
        f.booking_id,
        COUNT(DISTINCT btp.passenger_id) AS total_passengers
        
    FROM {{ ref('fact_ticket_transaction') }} f
    INNER JOIN {{ ref('bridge_ticket_passenger') }} btp ON f.ticket_id = btp.ticket_id
    GROUP BY 1
),

monthly_booking_base AS (
    -- 3. Combine Ticket and Passenger metrics at the booking level
    SELECT
        t.year_month_id,
        t.booking_id,
        t.total_booking_price,
        t.total_tickets,
        COALESCE(p.total_passengers, 0) AS total_passengers
        
    FROM booking_ticket_base t
    LEFT JOIN booking_passenger_base p ON t.booking_id = p.booking_id
),

booking_metrics AS (
    -- 4. Calculate final monthly aggregates
    SELECT
        year_month_id,
        COUNT(DISTINCT booking_id) AS total_bookings,
        SUM(total_booking_price) AS total_revenue_eur,
        SUM(total_tickets) AS total_tickets,
        SUM(total_passengers) AS total_passengers,
        
        AVG(total_tickets) AS avg_tickets_per_booking,
        AVG(total_passengers) AS avg_passengers_per_booking,
        SAFE_DIVIDE(SUM(total_booking_price), COUNT(DISTINCT booking_id)) AS avg_booking_value_eur,
        
        SUM(total_booking_price) AS total_ticket_revenue_eur,
        SAFE_DIVIDE(SUM(total_booking_price), SUM(total_tickets)) AS avg_ticket_price_eur
        
    FROM monthly_booking_base
    GROUP BY 1
),

monthly_segment_metrics AS (
    -- 5. Calculate unique segments (remains correct as it uses DISTINCT on the Bridge)
    SELECT
        d.year_month_id,
        COUNT(DISTINCT bs.segment_id) AS unique_segments
    FROM {{ ref('bridge_ticket_segment') }} bs
    INNER JOIN {{ ref('fact_ticket_transaction') }} f ON bs.ticket_id = f.ticket_id
    INNER JOIN {{ ref('dim_date') }} d ON f.ticket_issue_date_id = d.date_id
    GROUP BY 1
),

monthly_passenger_metrics AS (
    -- 6. Calculate total unique passengers in the month
    SELECT
        d.year_month_id,
        COUNT(DISTINCT btp.passenger_id) AS unique_passengers
    FROM {{ ref('bridge_ticket_passenger') }} btp
    INNER JOIN {{ ref('fact_ticket_transaction') }} f ON btp.ticket_id = f.ticket_id
    INNER JOIN {{ ref('dim_date') }} d ON f.ticket_issue_date_id = d.date_id
    GROUP BY 1
),

ratios AS (
    -- 7. Calculate ratios
    SELECT
        b.year_month_id,
        SAFE_DIVIDE(b.total_bookings, p.unique_passengers) AS avg_bookings_per_passenger,
        SAFE_DIVIDE(s.unique_segments, b.total_bookings) AS avg_segments_per_booking
    FROM booking_metrics b
    LEFT JOIN monthly_passenger_metrics p USING (year_month_id)
    LEFT JOIN monthly_segment_metrics s USING (year_month_id)
)

SELECT
    b.year_month_id,
    b.total_bookings,
    b.total_revenue_eur,
    b.total_tickets,
    b.total_passengers,
    b.avg_booking_value_eur,
    b.avg_tickets_per_booking,
    b.avg_passengers_per_booking,
    
    b.total_tickets AS total_tickets_unique,
    b.avg_ticket_price_eur,
    b.total_ticket_revenue_eur,
    
    p.unique_passengers,
    s.unique_segments,
    
    r.avg_bookings_per_passenger,
    r.avg_segments_per_booking
    
FROM booking_metrics b
LEFT JOIN monthly_passenger_metrics p USING (year_month_id)
LEFT JOIN monthly_segment_metrics s USING (year_month_id)
LEFT JOIN ratios r USING (year_month_id)

ORDER BY b.year_month_id