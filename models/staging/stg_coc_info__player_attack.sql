
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
FROM {{ ref('base_coc_info__player_attack') }}
ORDER BY attack_id