{{ config(
    materialized='table',
    tags=['silver','player_relationships']
) }}

WITH player_heroes AS (
    SELECT
        p.player_id,
        p.ingest_ts,
        hero.value:name::VARCHAR AS hero_name,
        hero.value:level::INT AS level,
        hero.value:maxLevel::INT AS max_level,
        hero.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }} p,
    LATERAL FLATTEN(input => p.raw:heroes) AS hero
    WHERE p.raw:heroes IS NOT NULL
),

joined_heroes AS (
    SELECT
        MD5(ph.player_id || '-' || h.hero_id) AS player_hero_id,
        ph.player_id,
        h.hero_id,
        ph.level,
        ph.max_level,
        ph.ingest_ts
    FROM player_heroes ph
    INNER JOIN {{ ref('stg_coc_raw_info__heroes') }} h
        ON ph.hero_name = h.name
        AND COALESCE(ph.village, 'home') = h.village
)

SELECT
    player_hero_id,
    player_id,
    hero_id,
    level,
    max_level,
    ingest_ts
FROM joined_heroes