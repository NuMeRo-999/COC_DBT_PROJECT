{{
    config(
        tags=['silver','player_relationships']
    )
}}

WITH player_hero_data AS (
    SELECT
        player_id,
        MD5(hero.value:name::VARCHAR || '-' || COALESCE(hero.value:village::VARCHAR, 'home')) AS hero_id,
        hero.value:level::INT AS level,
        hero.value:maxLevel::INT AS max_level,
        ingest_ts
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_heroes) AS hero
    WHERE raw_heroes IS NOT NULL
),

unique_player_heroes AS (
    SELECT
        MD5(player_id || '-' || hero_id) AS player_hero_id,
        player_id,
        hero_id,
        level,
        max_level,
        ingest_ts
    FROM player_hero_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id, hero_id
        ORDER BY ingest_ts DESC
    ) = 1
)

SELECT
    player_hero_id,
    player_id,
    hero_id,
    level,
    max_level,
    ingest_ts
FROM unique_player_heroes