{{ config(
    tags=['silver','player_relationships']
) }}

WITH hero_equipment_data AS (
    SELECT
        player_id,
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:level::INT AS level,
        equipment.value:maxLevel::INT AS max_level,
        equipment.value:village::VARCHAR AS village,
        ingest_ts
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_hero_equipment) AS equipment
    WHERE raw_hero_equipment IS NOT NULL
)

SELECT
    MD5(player_id || '-' || equipment_name || '-' || COALESCE(village, 'home')) AS player_hero_equipment_id,
    MD5(player_id || '-' || 'default-hero') AS player_hero_id,
    MD5(equipment_name || '-' || COALESCE(village, 'home')) AS hero_equipment_id,
    level,
    max_level,
    ingest_ts
FROM hero_equipment_data