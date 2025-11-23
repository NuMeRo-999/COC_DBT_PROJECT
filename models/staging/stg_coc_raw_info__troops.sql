{{ config(
    materialized='table',
    tags=['silver','troops']
) }}

WITH player_troops AS (
    SELECT
        player_id,
        ingest_ts,
        troop.value:name::VARCHAR AS troop_name,
        troop.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }},
    LATERAL FLATTEN(input => raw:troops) AS troop
    WHERE raw:troops IS NOT NULL
),

unique_troops AS (
    SELECT DISTINCT
        troop_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_troops
    WHERE troop_name IS NOT NULL
),

troops_with_id AS (
    SELECT
        MD5(name || '-' || village) AS troop_id,
        name,
        village
    FROM unique_troops
)

SELECT
    troop_id,
    name,
    village
FROM troops_with_id