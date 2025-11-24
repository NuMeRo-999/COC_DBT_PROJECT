
WITH player_spells AS (
    SELECT
        player_tag,
        ingest_ts,
        spell.value:name::VARCHAR AS spell_name,
        spell.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }},
    LATERAL FLATTEN(input => raw:spells) AS spell
    WHERE raw:spells IS NOT NULL
),

unique_spells AS (
    SELECT DISTINCT
        spell_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_spells
    WHERE spell_name IS NOT NULL
),

spells_with_id AS (
    SELECT
        MD5(name || '-' || village) AS spell_id,
        name,
        village
    FROM unique_spells
)

SELECT
    spell_id,
    name,
    village
FROM spells_with_id