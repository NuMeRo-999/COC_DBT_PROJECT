{{ config(
    materialized='table',
    tags=['silver','heroes']
) }}

WITH player_heroes AS (
    SELECT
        ingest_ts,
        hero.value:name::VARCHAR AS hero_name,
        hero.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }},
    LATERAL FLATTEN(input => raw:heroes) AS hero
    WHERE raw:heroes IS NOT NULL
),

unique_heroes AS (
    SELECT DISTINCT
        hero_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_heroes
    WHERE hero_name IS NOT NULL
),

heroes_with_id AS (
    SELECT
        MD5(name || '-' || village) AS hero_id,
        name,
        village
    FROM unique_heroes
)

SELECT
    hero_id,
    name,
    village
FROM heroes_with_id