{{ config(materialized='table', tags=['gold','business','top_players']) }}

WITH player_war_stats AS (
    SELECT
        pa.attacker_id as player_id,
        COUNT(pa.attack_id) as total_attacks,
        ROUND(AVG(pa.stars)) as avg_war_stars,
        ROUND(AVG(pa.destruction_percentage)) as avg_destruction,
        SUM(pa.stars) as total_war_stars
    FROM {{ ref('stg_coc_info__player_attack') }} pa
    GROUP BY pa.attacker_id
),

player_progression AS (
    SELECT
        player_id,
        MAX(town_hall_level) as current_th_level,
        MAX(exp_level) as current_exp_level,
        SUM(town_hall_change) as total_th_upgrades,
        SUM(exp_change) as total_exp_gain
    FROM {{ ref('fact_player_progression_daily') }}
    GROUP BY player_id
),

player_rankings AS (
    SELECT
        dp.player_id,
        dp.player_name,
        dp.clan_name,
        dp.town_hall_tier,
        pp.current_th_level,
        pp.current_exp_level,
        pp.total_th_upgrades,
        pp.total_exp_gain,
        COALESCE(pws.total_attacks, 0) as war_attacks,
        COALESCE(pws.avg_war_stars, 0) as avg_war_stars,
        COALESCE(pws.avg_destruction, 0) as avg_destruction,
        -- Puntuación compuesta
        (pp.current_th_level * 10) + 
        (pp.current_exp_level * 2) + 
        (COALESCE(pws.total_war_stars, 0) * 5) +
        (COALESCE(pws.avg_destruction, 0) * 0.1) as performance_score
    FROM {{ ref('dim_player') }} dp
    LEFT JOIN player_progression pp ON dp.player_id = pp.player_id
    LEFT JOIN player_war_stats pws ON dp.player_id = pws.player_id
    WHERE dp.clan_id IS NOT NULL
)

SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY performance_score DESC) as overall_rank,
    ROW_NUMBER() OVER (PARTITION BY town_hall_tier ORDER BY performance_score DESC) as tier_rank
FROM player_rankings
ORDER BY overall_rank
