{{ config(materialized='table', tags=['gold','facts','war']) }}

WITH war_attacks_agg AS (
    SELECT
        pa.clan_war_id,
        COUNT(pa.attack_id) as total_attacks,
        COUNT(DISTINCT pa.attacker_id) as unique_attackers,
        ROUND(AVG(pa.stars)) as avg_stars_per_attack,
        ROUND(AVG(pa.destruction_percentage)) as avg_destruction_per_attack,
        SUM(pa.stars) as total_stars_earned,
        SUM(pa.destruction_percentage) as total_destruction
    FROM {{ ref('stg_coc_info__player_attack') }} pa
    GROUP BY clan_war_id
),

war_performance AS (
    SELECT
        cw.clan_war_id,
        cw.clan_id,
        cw.opponent_clan_name,
        cw.team_size,
        cw.stars as clan_stars,
        cw.destruction_percentage as clan_destruction,
        cw.exp_earned,
        ws.state as war_result,
        cw.start_time,
        cw.end_time,
        wa.total_attacks,
        wa.unique_attackers,
        wa.avg_stars_per_attack,
        wa.avg_destruction_per_attack,
        wa.total_stars_earned,
        -- Métricas de eficiencia
        CASE 
            WHEN cw.team_size > 0 THEN 
                ROUND((cw.stars * 100.0) / (cw.team_size * 3), 2)
            ELSE 0 
        END as star_efficiency_percent,
        CASE 
            WHEN wa.total_attacks > 0 THEN 
                ROUND((cw.stars * 100.0) / wa.total_attacks, 2)
            ELSE 0 
        END as stars_per_attack_percent,
        -- Utilización de ataques
        ROUND((wa.total_attacks * 100.0) / (cw.team_size * cw.attacks_per_member), 2) as attack_utilization_percent
    FROM {{ ref('stg_coc_info__clan_war') }} cw
    LEFT JOIN {{ ref('stg_coc_info__war_state') }} ws ON cw.state_id = ws.state_id
    LEFT JOIN war_attacks_agg wa ON cw.clan_war_id = wa.clan_war_id
    WHERE opponent_clan_name != 'clan_inexistente'
    AND war_result != 'preparation'
)

SELECT *
FROM war_performance