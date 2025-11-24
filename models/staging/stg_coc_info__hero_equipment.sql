{{ config(
    materialized='table',
    tags=['silver','player_relationships']
) }}

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

final AS (
    SELECT
        MD5(player_tag || '-' || equipment_name) AS player_hero_equipment_id,
        MD5(player_tag || '-' || equipment_name || '-' || COALESCE(village, 'home')) AS hero_equipment_id,
        player_tag,
        equipment_name,
        level,
        max_level,
        village,
        CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
    FROM player_hero_equipment_data
)

SELECT
    player_hero_equipment_id,
    hero_equipment_id,
    equipment_name,
    level,
    max_level,
    village,
    ingest_ts
FROM final
