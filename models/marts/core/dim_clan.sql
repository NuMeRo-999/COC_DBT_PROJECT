{{ config(materialized='table', tags=['gold','core','dimension']) }}

SELECT
    clan_id,
    name as clan_name,
    type,
    description,
    location,
    is_family_friendly,
    clan_level,
    clan_points,
    war_frequency,
    war_win_streak,
    war_wins,
    war_ties,
    war_losses,
    war_league,
    members,
    -- Métricas calculadas
    CASE 
        WHEN (war_wins + war_losses + war_ties) > 0 THEN
            ROUND(war_wins * 100.0 / (war_wins + war_losses + war_ties), 2)
        ELSE 0
    END as war_win_rate,
    -- Categorías
    CASE 
        WHEN members = 50 THEN 'Full'
        WHEN members >= 45 THEN 'Almost Full'
        WHEN members >= 30 THEN 'Medium'
        ELSE 'Small'
    END as clan_size_category,
    ingest_ts
FROM {{ ref('stg_coc_info__clan') }}