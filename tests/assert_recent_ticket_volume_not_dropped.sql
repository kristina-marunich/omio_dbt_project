WITH daily_ticket_counts AS (
    -- Count tickets per day from the staging layer
    SELECT
        DATE(uploaded_at_timestamp) AS ticket_date,
        COUNT(ticket_id) AS daily_count
    FROM {{ ref('stg_backend__ticket') }}
    GROUP BY 1
),

comparison AS (
    SELECT
        ticket_date,
        daily_count,
        -- Calculate the average volume for the 7 days PRIOR to (and excluding) the current ticket_date
        AVG(daily_count) OVER (ORDER BY ticket_date ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING) AS avg_7_day_volume
    FROM daily_ticket_counts
),

latest_day_check AS (
    -- Focus only on the most recent fully loaded day
    SELECT *
    FROM comparison
    ORDER BY ticket_date DESC
    LIMIT 1
)

SELECT
    *
FROM latest_day_check
-- Fails if today's volume is less than 50% (half) of the prior 7-day average
WHERE daily_count < avg_7_day_volume * 0.5