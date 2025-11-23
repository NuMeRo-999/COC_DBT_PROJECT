{{ config(
    materialized='table',
    tags=['silver','player_relationships']
) }}

WITH player_spells AS (
    SELECT
        p.player_id,
        p.ingest_ts,
        spell.value:name::VARCHAR AS spell_name,
        spell.value:level::INT AS level,
        spell.value:maxLevel::INT AS max_level,
        spell.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }} p,
    LATERAL FLATTEN(input => p.raw:spells) AS spell
    WHERE p.raw:spells IS NOT NULL
),

joined_spells AS (
    SELECT
        MD5(ps.player_id || '-' || s.spell_id) AS player_spell_id,
        ps.player_id,
        s.spell_id,
        ps.level,
        ps.max_level,
        ps.ingest_ts
    FROM player_spells ps
    INNER JOIN {{ ref('stg_coc_raw_info__spells') }} s
        ON ps.spell_name = s.name
        AND COALESCE(ps.village, 'home') = s.village
)

SELECT
    player_spell_id,
    player_id,
    spell_id,
    level,
    max_level,
    ingest_ts
FROM joined_spells