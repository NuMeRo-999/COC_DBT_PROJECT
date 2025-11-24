
WITH player_achievements AS (
    SELECT
        ingest_ts,
        achievement.value:name::VARCHAR AS achievement_name,
        achievement.value:target::INT AS target,
        achievement.value:info::VARCHAR AS info,
        achievement.value:completionInfo::VARCHAR AS completion_info,
        achievement.value:village::VARCHAR AS village
    FROM {{ source('coc_raw_info', 'player_raw') }},
    LATERAL FLATTEN(input => raw:achievements) AS achievement
    WHERE raw:achievements IS NOT NULL
),

unique_achievements AS (
    SELECT DISTINCT
        achievement_name AS name,
        target,
        info,
        completion_info,
        COALESCE(village, 'home') AS village
    FROM player_achievements
    WHERE achievement_name IS NOT NULL
),

achievements_with_id AS (
    SELECT
        MD5(name || '-' || COALESCE(village, 'home')) AS achievement_id,
        name,
        target,
        info,
        completion_info,
        village
    FROM unique_achievements
)

SELECT
    achievement_id,
    name,
    target,
    info,
    completion_info,
    village
FROM achievements_with_id