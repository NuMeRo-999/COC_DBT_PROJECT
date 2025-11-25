{{ config(
    tags=['silver','heroes']
) }}

WITH player_heroes AS (
    SELECT
        hero.value:name::VARCHAR AS hero_name,
        hero.value:village::VARCHAR AS village
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_heroes) AS hero
    WHERE raw_heroes IS NOT NULL
),

unique_heroes AS (
    SELECT DISTINCT
        hero_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_heroes
    WHERE hero_name IS NOT NULL
)

SELECT
    MD5(name || '-' || village) AS hero_id,
    name,
    village
FROM unique_heroes