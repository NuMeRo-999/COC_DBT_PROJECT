{{ config(
    materialized='table',
    tags=['silver','player_relationships']
) }}

WITH player_troops AS (
    SELECT
        p.player_id,
        p.ingest_ts,
        troop.value:name::VARCHAR AS troop_name,
        troop.value:level::INT AS level,
        troop.value:maxLevel::INT AS max_level,
        troop.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }} p,
    LATERAL FLATTEN(input => p.raw:troops) AS troop
    WHERE p.raw:troops IS NOT NULL
),

joined_troops AS (
    SELECT
        MD5(pt.player_id || '-' || t.troop_id) AS player_troop_id,
        pt.player_id,
        t.troop_id,
        pt.level,
        pt.max_level,
        pt.ingest_ts
    FROM player_troops pt
    INNER JOIN {{ ref('stg_coc_raw_info__troops') }} t
        ON pt.troop_name = t.name
        AND COALESCE(pt.village, 'home') = t.village
)

SELECT
    player_troop_id,
    player_id,
    troop_id,
    level,
    max_level,
    ingest_ts
FROM joined_troops