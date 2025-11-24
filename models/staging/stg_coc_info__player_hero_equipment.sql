/*
{{ config(
    materialized='incremental',
) }}
*/


WITH player_hero_equipment_data AS (
    SELECT
        p.player_tag,
        p.ingest_ts,
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:level::INT AS level,
        equipment.value:maxLevel::INT AS max_level,
        equipment.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }} p,
    LATERAL FLATTEN(input => p.raw:heroEquipment) AS equipment
    WHERE p.raw:heroEquipment IS NOT NULL
),

-- Join with player_hero to get the player_hero_id
player_hero_equipment_with_ids AS (
    SELECT
        MD5(pe.player_tag || '-' || pe.equipment_name || '-' || COALESCE(pe.village, 'home')) AS player_hero_equipment_id,
        COALESCE(ph.player_hero_id, MD5(pe.player_tag || '-unknown-hero')) AS player_hero_id,
        pe.equipment_name,
        pe.level,
        pe.max_level,
        pe.ingest_ts
    FROM player_hero_equipment_data pe
    LEFT JOIN {{ ref('stg_coc_info__player_hero') }} ph
        ON pe.player_tag = ph.player_id
),

joined_equipment AS (
    SELECT
        phe.player_hero_equipment_id,
        phe.player_hero_id,
        he.hero_equipment_id,
        phe.level,
        phe.max_level,
        phe.ingest_ts
    FROM player_hero_equipment_with_ids phe
    INNER JOIN {{ ref('stg_coc_info__hero_equipment') }} he
        ON phe.equipment_name = he.equipment_name
)

SELECT
    player_hero_equipment_id,
    player_hero_id,
    hero_equipment_id,
    level,
    max_level,
    CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
FROM joined_equipment