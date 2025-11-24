{{ config(
    tags=['silver','player_relationships']
) }}

SELECT
    MD5(player_id || '-' || achievement.value:name::VARCHAR || '-' || COALESCE(achievement.value:village::VARCHAR, 'home')) AS player_achievement_id,
    player_id,
    MD5(achievement.value:name::VARCHAR || '-' || COALESCE(achievement.value:village::VARCHAR, 'home')) AS achievement_id,
    achievement.value:value::INT AS value,
    achievement.value:stars::INT AS stars,
    ingest_ts
FROM {{ ref('base_coc_info__player') }},
LATERAL FLATTEN(input => raw_achievements) AS achievement
WHERE raw_achievements IS NOT NULL