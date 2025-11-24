{{ config(
    materialized='incremental',
) }}

WITH attack_data AS (
    SELECT
        MD5(
            raw:attackerTag::VARCHAR || '-' || 
            raw:defenderTag::VARCHAR || '-' || 
            COALESCE(raw:attackNumber::VARCHAR, '1') || '-' ||
            COALESCE(raw:mapPosition::VARCHAR, '0')
        ) AS attack_id,
        clan_war_id,
        raw:attackerTag::VARCHAR AS attacker_id,
        raw:defenderTag::VARCHAR AS defender_tag,
        raw:stars::INT AS stars,
        raw:destructionPercentage::FLOAT AS destruction_percentage,
        COALESCE(raw:attackNumber::INT, 1) AS attack_number,
        COALESCE(raw:mapPosition::INT, 0) AS map_position,
        raw:duration::INT AS duration,
        CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
    FROM {{ source('coc_raw_info', 'attack_raw') }} a
    INNER JOIN {{ ref('stg_coc_info__clan_war') }} cw
    WHERE raw IS NOT NULL
)

SELECT
    attack_id,
    clan_war_id,
    attacker_id,
    defender_tag,
    stars,
    destruction_percentage,
    attack_number,
    map_position,
    duration,
    ingest_ts
FROM attack_data