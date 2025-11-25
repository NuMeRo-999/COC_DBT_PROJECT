{{
    config(
        tags=['silver','hero_equipment']
    )
}}

WITH hero_equipment_data AS (
    SELECT
        equipment.value:name::VARCHAR AS equipment_name,
        equipment.value:village::VARCHAR AS equipment_village,
        hero.value:name::VARCHAR AS hero_name,
        hero.value:village::VARCHAR AS hero_village
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_heroes) AS hero,
    LATERAL FLATTEN(input => hero.value:equipment) AS equipment
    WHERE raw_heroes IS NOT NULL
      AND hero.value:equipment IS NOT NULL
      AND equipment.value:name IS NOT NULL
),

unique_equipment AS (
    SELECT DISTINCT
        equipment_name AS name,
        COALESCE(equipment_village, 'home') AS village,
        hero_name,
        COALESCE(hero_village, 'home') AS hero_village
    FROM hero_equipment_data
)

SELECT
    MD5(name || '-' || village) AS hero_equipment_id,
    MD5(hero_name || '-' || hero_village) AS hero_id,
    name,
    village
FROM unique_equipment
ORDER BY name