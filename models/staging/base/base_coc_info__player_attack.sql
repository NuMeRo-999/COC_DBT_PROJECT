{{
    config(
        materialized='incremental',
        tags=['silver','attack','base']
    )
}}

WITH base_attacks AS (
    SELECT
        raw:attackerTag::VARCHAR AS attacker_tag,
        raw:defenderTag::VARCHAR AS defender_tag,
        raw:stars::INT AS stars,
        raw:destructionPercentage::FLOAT AS destruction_percentage,
        COALESCE(raw:attackNumber::INT, 1) AS attack_number,
        COALESCE(raw:mapPosition::INT, 0) AS map_position,
        raw:duration::INT AS duration,
        raw:ingest_ts::TIMESTAMP AS ingest_ts,
        raw:warId::VARCHAR AS war_id_from_json,
        raw:teamSize::INT AS team_size,
        raw:warStartTime::VARCHAR AS war_start_time,
        raw:warEndTime::VARCHAR AS war_end_time
    FROM {{ source('coc_raw_info', 'attack_raw') }}
    WHERE raw IS NOT NULL
),

final_attacks AS (
    SELECT
        MD5(
            COALESCE(war_id_from_json::VARCHAR, 'NO_WAR') || '|' ||
            ba.attacker_tag || '|' || 
            ba.defender_tag || '|' || 
            ba.attack_number::VARCHAR || '|' ||
            ba.map_position::VARCHAR || '|' ||
            ba.stars::VARCHAR || '|' ||
            ROUND(ba.destruction_percentage, 2)::VARCHAR || '|' ||
            ba.duration::VARCHAR || '|' ||
            COALESCE(ba.team_size::VARCHAR, '0') || '|' ||
            COALESCE(ba.war_start_time::VARCHAR, 'NO_START') || '|' ||
            COALESCE(ba.war_end_time::VARCHAR, 'NO_END')
        ) AS attack_id,
        war_id_from_json AS clan_war_id,
        MD5(ba.attacker_tag) AS attacker_id,
        ba.defender_tag,
        ba.stars,
        ba.destruction_percentage,
        ba.attack_number,
        ba.map_position,
        ba.duration,
        CONVERT_TIMEZONE('UTC', ingest_ts) AS ingest_ts
    FROM base_attacks ba
    {% if is_incremental() %}
        WHERE ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }})
    {% endif %}
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
FROM final_attacks