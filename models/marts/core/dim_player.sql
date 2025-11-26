{{ config(materialized='table', tags=['gold','core','dimension']) }}

WITH player_enriched AS (
    SELECT
        p.player_id,
        p.name as player_name,
        p.town_hall_level,
        p.exp_level,
        p.role,
        p.war_preference,
        p.clan_id,
        c.name as clan_name,
        c.clan_level,
        c.war_league,
        c.war_frequency,
        -- Categorías
        CASE 
            WHEN p.town_hall_level >= 14 THEN 'TH14+'
            WHEN p.town_hall_level >= 12 THEN 'TH12-13'
            WHEN p.town_hall_level >= 10 THEN 'TH10-11'
            ELSE 'TH1-9'
        END as town_hall_tier,
        CASE 
            WHEN p.role IN ('leader', 'coLeader') THEN 'Leadership'
            WHEN p.role = 'elder' THEN 'Elder'
            ELSE 'Member'
        END as role_category,
        p.ingest_ts
    FROM {{ ref('stg_coc_info__player') }} p
    LEFT JOIN {{ ref('stg_coc_info__clan') }} c ON p.clan_id = c.clan_id
)
SELECT *
FROM player_enriched