{{
    config(
        tags=['silver','player_relationships']
    )
}}

WITH hero_specific_equipment AS (
    SELECT
        player_id,
        hero.value:name::VARCHAR AS hero_name,
        hero.value:village::VARCHAR AS hero_village,
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:level::INT AS level,
        equipment.value:maxLevel::INT AS max_level,
        equipment.value:village::VARCHAR AS equipment_village,
        ingest_ts
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_heroes) AS hero,
    LATERAL FLATTEN(input => hero.value:equipment) AS equipment
    WHERE raw_heroes IS NOT NULL
      AND hero.value:equipment IS NOT NULL
)

SELECT
    MD5(player_id || '-' || equipment_name || '-' || COALESCE(equipment_village, 'home')) AS player_hero_equipment_id,
    MD5(player_id || '-' || MD5(hero_name || '-' || COALESCE(hero_village, 'home'))) AS player_hero_id,
    MD5(equipment_name || '-' || COALESCE(equipment_village, 'home')) AS hero_equipment_id,
    level,
    max_level,
    ingest_ts
FROM hero_specific_equipment