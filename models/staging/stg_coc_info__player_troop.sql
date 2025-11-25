{{ config(
    tags=['silver','player_relationships']
) }}

SELECT 
    MD5(player_id || '-' || troop.value:name::VARCHAR || '-' || COALESCE(troop.value:village::VARCHAR, 'home')) AS player_troop_id,
    player_id,
    MD5(troop.value:name::VARCHAR || '-' || COALESCE(troop.value:village::VARCHAR, 'home')) AS troop_id,
    troop.value:level::INT AS level,
    troop.value:maxLevel::INT AS max_level,
    ingest_ts
FROM {{ ref('base_coc_info__player') }},
LATERAL FLATTEN(input => raw_troops) AS troop
WHERE raw_troops IS NOT NULL