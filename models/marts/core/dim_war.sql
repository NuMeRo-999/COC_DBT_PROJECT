{{ config(materialized='table', tags=['gold','core','dimension']) }}

SELECT
    cw.clan_war_id,
    cw.clan_id,
    c.name as clan_name,
    cw.opponent_clan_name,
    cw.team_size,
    cw.attacks_per_member,
    ws.state as war_state,
    cw.start_time,
    cw.end_time,
    cw.battle_modifier,
    DATEDIFF('hour', cw.start_time, cw.end_time) as war_duration_hours,
    CASE 
        WHEN HOUR(cw.start_time) BETWEEN 6 AND 18 THEN 'Day'
        ELSE 'Night'
    END as war_start_time_of_day,
    CASE 
        WHEN cw.team_size = 5 THEN 'Small War'
        WHEN cw.team_size = 15 THEN 'Medium War'
        WHEN cw.team_size = 30 THEN 'Large War'
        WHEN cw.team_size > 30  THEN 'League War'
        ELSE 'Small War'
    END as war_size_category
FROM {{ ref('stg_coc_info__clan_war') }} cw
LEFT JOIN {{ ref('stg_coc_info__clan') }} c ON cw.clan_id = c.clan_tag
LEFT JOIN {{ ref('stg_coc_info__war_state') }} ws ON cw.state_id = ws.state_id