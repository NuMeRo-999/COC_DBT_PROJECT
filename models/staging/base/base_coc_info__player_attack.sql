WITH base_attacks AS (
    SELECT
        raw:attackerTag::VARCHAR AS attacker_tag,
        raw:defenderTag::VARCHAR AS defender_tag,
        raw:stars::INT AS stars,
        raw:destructionPercentage::FLOAT AS destruction_percentage,
        COALESCE(raw:attackNumber::INT, 1) AS attack_number,
        COALESCE(raw:mapPosition::INT, 0) AS map_position,
        raw:duration::INT AS duration,
        clan_tag AS clan_id,
        raw:ingest_ts::TIMESTAMP AS attack_ts
    FROM {{ source('coc_raw_info', 'attack_raw') }}
    WHERE raw IS NOT NULL
),

wars_lookup AS (
    SELECT
        clan_war_id,
        clan_id,
        start_time,
        end_time
    FROM {{ ref('base_coc_info__clan_war') }}
),

attacks_with_war_id AS (
    SELECT
        a.*,
        COALESCE(
            w.clan_war_id,
            MD5(a.clan_id || '-' || COALESCE(a.attack_ts::DATE::VARCHAR, 'unknown'))
        ) AS clan_war_id
    FROM base_attacks a
    LEFT JOIN wars_lookup w 
        ON a.clan_id = w.clan_id 
        AND a.attack_ts BETWEEN w.start_time AND w.end_time
),

final_attacks AS (
    SELECT
        MD5(
            attacker_tag || '-' || 
            defender_tag || '-' || 
            attack_number::VARCHAR || '-' ||
            map_position::VARCHAR || '-' ||
            COALESCE(attack_ts::VARCHAR, CURRENT_TIMESTAMP::VARCHAR)
        ) AS attack_id,
        clan_war_id,
        attacker_tag AS attacker_id,
        defender_tag,
        stars,
        destruction_percentage,
        attack_number,
        map_position,
        duration,
        clan_id,
        ingest_ts
    FROM attacks_with_war_id
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
    clan_id,
    CONVERT_TIMEZONE('UTC', current_timestamp()) AS ingest_ts
FROM final_attacks