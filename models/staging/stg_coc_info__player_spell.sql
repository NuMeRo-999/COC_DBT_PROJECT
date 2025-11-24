{{ config(
    tags=['silver','player_relationships']
) }}

SELECT
    MD5(player_id || '-' || spell.value:name::VARCHAR || '-' || COALESCE(spell.value:village::VARCHAR, 'home')) AS player_spell_id,
    player_id,
    MD5(spell.value:name::VARCHAR || '-' || COALESCE(spell.value:village::VARCHAR, 'home')) AS spell_id,
    spell.value:level::INT AS level,
    spell.value:maxLevel::INT AS max_level,
    ingest_ts
FROM {{ ref('base_coc_info__player') }},
LATERAL FLATTEN(input => raw_spells) AS spell
WHERE raw_spells IS NOT NULL