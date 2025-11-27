{{ config(materialized='table', tags=['gold','business','war_analytics']) }}

WITH war_base AS (
    SELECT
        dd.year,
        dd.month,
        dd.month_name,
        dw.war_size_category,
        dw.clan_war_id,
        dw.war_state,
        fwp.star_efficiency_percent,
        fwp.attack_utilization_percent,
        fwp.avg_stars_per_attack,
        fwp.exp_earned
    FROM {{ ref('fact_war_performance') }} fwp
    JOIN {{ ref('dim_war') }} dw ON fwp.clan_war_id = dw.clan_war_id
    JOIN {{ ref('dim_date') }} dd ON DATE_TRUNC('day', dw.start_time) = dd.date_day
),

war_aggregated AS (
    SELECT
        year,
        month,
        month_name,
        war_size_category,
        COUNT(DISTINCT clan_war_id) as total_wars,
        SUM(CASE WHEN war_state = 'win' THEN 1 ELSE 0 END) as wars_won,
        SUM(CASE WHEN war_state = 'lose' THEN 1 ELSE 0 END) as wars_lost,
        SUM(CASE WHEN war_state = 'tie' THEN 1 ELSE 0 END) as wars_tied,
        AVG(CASE WHEN war_state IN ('win', 'lose', 'tie') THEN star_efficiency_percent ELSE NULL END) as avg_star_efficiency,
        AVG(CASE WHEN war_state IN ('win', 'lose', 'tie') THEN attack_utilization_percent ELSE NULL END) as avg_attack_utilization,
        AVG(CASE WHEN war_state IN ('win', 'lose', 'tie') THEN avg_stars_per_attack ELSE NULL END) as avg_stars_per_attack,
        SUM(exp_earned) as total_exp_earned
    FROM war_base
    GROUP BY 1, 2, 3, 4
)

SELECT
    year,
    month,
    month_name,
    war_size_category,
    total_wars,
    wars_won,
    wars_lost,
    wars_tied,

    ROUND(wars_won * 100.0 / total_wars, 2) as win_rate_total,
    ROUND(COALESCE(avg_star_efficiency, 0), 2) as star_efficiency,
    ROUND(COALESCE(avg_attack_utilization, 0), 2) as attack_utilization,
    ROUND(COALESCE(avg_stars_per_attack, 0), 2) as stars_per_attack,
    total_exp_earned,
    ROUND(wars_won * 100.0 / total_wars, 2) as win_percentage,
    ROUND(wars_lost * 100.0 / total_wars, 2) as loss_percentage,
    ROUND(wars_tied * 100.0 / total_wars, 2) as tie_percentage
FROM war_aggregated
ORDER BY year DESC, month DESC, war_size_category
