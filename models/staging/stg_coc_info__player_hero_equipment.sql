{{ config(
    tags=['silver','hero_equipment']
) }}

WITH player_hero_equipment_data AS (
    SELECT
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:village::VARCHAR AS village
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_hero_equipment) AS equipment
    WHERE raw_hero_equipment IS NOT NULL
),

unique_equipment AS (
    SELECT DISTINCT
        equipment_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_hero_equipment_data
    WHERE equipment_name IS NOT NULL
)

SELECT
    MD5(name || '-' || village) AS hero_equipment_id,
    name,
    village
FROM unique_equipment