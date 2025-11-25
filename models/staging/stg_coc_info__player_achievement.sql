{{
    config(
        tags=['silver','player_relationships']
    )
}}

WITH player_achievement_data AS (
    SELECT
        player_id,
        MD5(achievement.value:name::VARCHAR || '-' || COALESCE(achievement.value:village::VARCHAR, 'home')) AS achievement_id,
        achievement.value:value::INT AS value,
        achievement.value:stars::INT AS stars,
        ingest_ts
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_achievements) AS achievement
    WHERE raw_achievements IS NOT NULL
),

unique_player_achievements AS (
    SELECT
        MD5(player_id || '-' || achievement_id) AS player_achievement_id,
        player_id,
        achievement_id,
        value,
        stars,
        ingest_ts
    FROM player_achievement_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id, achievement_id
        ORDER BY ingest_ts DESC
    ) = 1
)

SELECT
    player_achievement_id,
    player_id,
    achievement_id,
    value,
    stars,
    ingest_ts
FROM unique_player_achievements