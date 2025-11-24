{{ config(
    tags=['silver','spells']
) }}

WITH player_spells AS (
    SELECT
        spell.value:name::VARCHAR AS spell_name,
        spell.value:village::VARCHAR AS village
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_spells) AS spell
    WHERE raw_spells IS NOT NULL
),

unique_spells AS (
    SELECT DISTINCT
        spell_name AS name,
        COALESCE(village, 'home') AS village
    FROM player_spells
    WHERE spell_name IS NOT NULL
)

SELECT
    MD5(name || '-' || village) AS spell_id,
    name,
    village
FROM unique_spells