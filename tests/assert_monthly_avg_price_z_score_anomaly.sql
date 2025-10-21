with
    monthly_avg as (
        -- Calculate the average price for each month in history
        select d.year_month_id, avg(f.ticket_price_eur) as monthly_avg_price
        from {{ ref("fact_ticket_transaction") }} f
        inner join {{ ref("dim_date") }} d on f.ticket_issue_date_id = d.date_id
        group by 1
    ),

    anomaly_check as (
        select
            year_month_id,
            monthly_avg_price,

            -- Calculate the statistical metrics over the entire history
            avg(monthly_avg_price) over () as historical_mean,
            stddev(monthly_avg_price) over () as historical_stddev

        from monthly_avg
    ),

    z_score_calculation as (
        select
            year_month_id,
            monthly_avg_price,
            -- Calculate the Z-Score: (Value - Mean) / Standard Deviation
            (monthly_avg_price - historical_mean) / historical_stddev as z_score
        from anomaly_check
        -- Avoid dividing by zero if the price has been perfectly stable (stddev = 0)
        where historical_stddev > 0
    )

select *
from z_score_calculation
-- Fails if the latest month's Z-Score is > 3 (3 standard deviations above the mean)
where
    year_month_id = (select max(year_month_id) from z_score_calculation) and z_score > 3
