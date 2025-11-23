{{ config(
    materialized='view',
    tags=['silver','player_relationships']
) }}

WITH player_hero_equipment AS (
    SELECT
        ph.player_hero_id,
        ph.player_id,
        ph.ingest_ts,
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:level::INT AS level,
        equipment.value:maxLevel::INT AS max_level,
        equipment.value:village::VARCHAR AS village
    FROM {{ ref('stg_coc_raw_info__player_hero') }} ph
    INNER JOIN {{ ref('stg_coc_raw_info__player') }} p
        ON ph.player_id = p.player_id,
    LATERAL FLATTEN(input => p.hero_equipment) AS equipment
    WHERE p.hero_equipment IS NOT NULL
),

joined_equipment AS (
    SELECT
        MD5(phe.player_hero_id || '-' || he.hero_equipment_id) AS player_hero_equipment_id,
        phe.player_hero_id,
        he.hero_equipment_id,
        phe.level,
        phe.max_level,
        phe.ingest_ts
    FROM player_hero_equipment phe
    INNER JOIN {{ ref('stg_coc_raw_info__hero_equipment') }} he
        ON phe.equipment_name = he.name
        AND COALESCE(phe.village, 'home') = COALESCE(he.village, 'home')
)

SELECT
    player_hero_equipment_id,
    player_hero_id,
    hero_equipment_id,
    level,
    max_level,
    ingest_ts
FROM joined_equipment