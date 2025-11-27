{{ config(materialized='table', tags=['gold','business','retention']) }}

WITH player_first_last_activity AS (
    SELECT
        player_id,
        MIN(date_key) as first_activity_date,
        MAX(date_key) as last_activity_date,
        COUNT(DISTINCT date_key) as active_days
    FROM {{ ref('fact_player_progression_daily') }}
    GROUP BY 1
),

player_changes AS (
    SELECT
        player_id,
        SUM(ABS(trophies_change)) as total_trophy_changes,
        SUM(ABS(exp_change)) as total_exp_changes,
        SUM(town_hall_upgrade_flag) as total_th_upgrades,
        MAX(war_stars) as total_war_stars
    FROM {{ ref('fact_player_progression_daily') }}
    GROUP BY 1
),

retention_analysis AS (
    SELECT
        dp.player_id,
        dp.player_name,
        dp.clan_name,
        dp.town_hall_tier,
        pla.first_activity_date,
        pla.last_activity_date,
        pla.active_days,
        DATEDIFF('day', pla.first_activity_date, pla.last_activity_date) as days_since_first_activity,
        DATEDIFF('day', pla.last_activity_date, CURRENT_DATE()) as days_since_last_activity,
        pc.total_th_upgrades,
        pc.total_war_stars,
        -- Engagement Score
        CASE 
            WHEN pla.active_days >= 30 AND pc.total_th_upgrades > 2 THEN 'High'
            WHEN pla.active_days >= 15 OR pc.total_th_upgrades > 0 THEN 'Medium'
            ELSE 'Low'
        END as engagement_level,
        -- Retention Risk
        CASE 
            WHEN days_since_last_activity > 30 THEN 'Inactive'
            WHEN days_since_last_activity > 14 THEN 'At Risk'
            ELSE 'Active'
        END as retention_status
    FROM {{ ref('dim_player') }} dp
    JOIN player_first_last_activity pla ON dp.player_id = pla.player_id
    JOIN player_changes pc ON dp.player_id = pc.player_id
)

SELECT *
FROM retention_analysis
ORDER BY days_since_last_activity DESC, engagement_level