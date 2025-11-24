{{ config(
    tags=['silver','player_relationships']
) }}

SELECT
    MD5(player_id || '-' || hero.value:name::VARCHAR || '-' || COALESCE(hero.value:village::VARCHAR, 'home')) AS player_hero_id,
    player_id,
    MD5(hero.value:name::VARCHAR || '-' || COALESCE(hero.value:village::VARCHAR, 'home')) AS hero_id,
    hero.value:level::INT AS level,
    hero.value:maxLevel::INT AS max_level,
    ingest_ts
FROM {{ ref('base_coc_info__player') }},
LATERAL FLATTEN(input => raw_heroes) AS hero
WHERE raw_heroes IS NOT NULL