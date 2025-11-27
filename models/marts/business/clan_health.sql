{{ config(materialized='table', tags=['gold','business','clan_health']) }}

WITH clan_activity AS (
    SELECT
        dc.clan_id,
        dc.clan_name,
        dc.members,
        dc.war_win_rate,
        COUNT(DISTINCT dp.player_id) as active_players,
        ROUND(AVG(dp.town_hall_level), 2) as avg_town_hall_level,
        ROUND(AVG(dp.exp_level), 2) as avg_exp_level,
        SUM(fpp.donations_daily) as total_donations_last_30d,
        SUM(ABS(fpp.net_donations)) as total_net_donations_last_30d
    FROM {{ ref('dim_clan') }} dc
    LEFT JOIN {{ ref('dim_player') }} dp ON dc.clan_id = dp.clan_id
    LEFT JOIN {{ ref('fact_player_progression_daily') }} fpp ON dp.player_id = fpp.player_id
    WHERE fpp.date_key >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1, 2, 3, 4
),

war_performance AS (
    SELECT
        dw.clan_id,
        COUNT(DISTINCT dw.clan_war_id) as total_wars_last_30d,
        ROUND(AVG(fwp.star_efficiency_percent), 2) as avg_star_efficiency,
        ROUND(AVG(fwp.attack_utilization_percent), 2) as avg_attack_utilization
    FROM {{ ref('dim_war') }} dw
    JOIN {{ ref('fact_war_performance') }} fwp ON dw.clan_war_id = fwp.clan_war_id
    WHERE dw.start_time >= DATEADD('day', -30, CURRENT_DATE())
    GROUP BY 1
)

SELECT
    ca.*,
    COALESCE(wp.total_wars_last_30d, 0) as recent_war_count,
    ROUND(COALESCE(wp.avg_star_efficiency, 0), 2) as recent_war_efficiency,
    COALESCE(wp.avg_attack_utilization, 0) as recent_attack_utilization,
    -- Health Score (0-100)
    ROUND(
        (ca.war_win_rate * 0.3) +
        (COALESCE(wp.avg_star_efficiency, 0) * 0.3) +
        (COALESCE(wp.avg_attack_utilization, 0) * 0.2) +
        (LEAST(ca.active_players * 100.0 / ca.members, 100) * 0.2),
        2
    ) as clan_health_score
FROM clan_activity ca
LEFT JOIN war_performance wp ON ca.clan_id = MD5(wp.clan_id)
ORDER BY clan_health_score DESC