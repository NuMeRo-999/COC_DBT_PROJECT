{{ config(materialized='table', tags=['gold','core','dimension']) }}

WITH date_series AS (
    SELECT
        DATEADD('day', seq4(), '2020-01-01'::DATE) as date_day
    FROM TABLE(GENERATOR(ROWCOUNT => 365*10)) -- 10 años
),
enriched_dates AS (
    SELECT
        date_day,
        YEAR(date_day) as year,
        QUARTER(date_day) as quarter,
        MONTH(date_day) as month,
        WEEK(date_day) as week,
        DAY(date_day) as day,
        DAYOFWEEK(date_day) as day_of_week,
        DAYNAME(date_day) as day_name,
        MONTHNAME(date_day) as month_name,
        CASE 
            WHEN day_of_week IN (1,7) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        date_day = CURRENT_DATE() as is_current_date,
        date_day < CURRENT_DATE() as is_past_date
    FROM date_series
)
SELECT
    MD5(date_day::varchar) as date_key,
    *
FROM enriched_dates