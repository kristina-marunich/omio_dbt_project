
WITH date_series AS (
    -- Generates a series of dates starting from a predefined start date
    SELECT
        DATE_ADD(DATE('2023-01-01'), INTERVAL ROW_NUMBER() OVER (ORDER BY 1) - 1 DAY) AS date_day
    FROM 
        -- Creates a large enough set of rows (e.g., 2000 days is about 5.5 years)
        UNNEST(GENERATE_ARRAY(1, 2000)) 
),

date_attributes AS (
    SELECT
        date_day,
        -- PK: Integer representation for efficient joining (YYYYMMDD)
        CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT) AS date_id, 

        -- Standard Date Attributes
        EXTRACT(YEAR FROM date_day) AS year_number,
        EXTRACT(MONTH FROM date_day) AS month_number,
        FORMAT_DATE('%B', date_day) AS month_name,
        CAST(FORMAT_DATE('%Y%m', date_day) AS INT) AS year_month_id, -- Used for your agg_monthly_summary
        
        EXTRACT(DAY FROM date_day) AS day_of_month,
        EXTRACT(WEEK FROM date_day) AS week_of_year,
        
        FORMAT_DATE('%A', date_day) AS day_of_week_name,
        EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week_number, -- 1=Sunday, 7=Saturday

        -- Boolean Flag
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend
        
    FROM date_series
)

SELECT * FROM date_attributes