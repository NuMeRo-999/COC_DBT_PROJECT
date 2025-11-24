/*
{{ config(
    materialized='incremental',
) }}
*/

WITH player_achievements AS (
    SELECT
        MD5(p.player_tag) AS player_id,
        p.player_tag,
        p.ingest_ts,
        achievement.value:name::VARCHAR AS achievement_name,
        achievement.value:value::INT AS value,
        achievement.value:stars::INT AS stars,
        achievement.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }} p,
    LATERAL FLATTEN(input => p.raw:achievements) AS achievement
    WHERE p.raw:achievements IS NOT NULL
),

joined_achievements AS (
    SELECT
        MD5(pa.player_tag || '-' || a.achievement_id) AS player_achievement_id,
        pa.player_id,
        a.achievement_id,
        pa.value,
        pa.stars,
        pa.ingest_ts
    FROM player_achievements pa
    INNER JOIN {{ ref('stg_coc_info__achievements') }} a
        ON pa.achievement_name = a.name
        AND COALESCE(pa.village, 'home') = a.village
)

SELECT
    player_achievement_id,
    player_id,
    achievement_id,
    value,
    stars,
    CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
FROM joined_achievements