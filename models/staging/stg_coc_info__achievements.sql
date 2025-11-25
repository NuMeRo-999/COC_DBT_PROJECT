{{
    config(
        tags=['silver','achievements']
    )
}}

WITH player_achievements AS (
    SELECT
        achievement.value:name::VARCHAR AS achievement_name,
        achievement.value:target::INT AS target,
        achievement.value:info::VARCHAR AS info,
        achievement.value:completionInfo::VARCHAR AS completion_info,
        achievement.value:village::VARCHAR AS village
    FROM {{ ref('base_coc_info__player') }},
    LATERAL FLATTEN(input => raw_achievements) AS achievement
    WHERE raw_achievements IS NOT NULL
),

unique_achievements AS (
    SELECT 
        achievement_name AS name,
        target,
        info,
        completion_info,
        COALESCE(village, 'home') AS village
    FROM player_achievements
    WHERE achievement_name IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY achievement_name, COALESCE(village, 'home')
        ORDER BY achievement_name
    ) = 1
)

SELECT
    MD5(name || '-' || village) AS achievement_id,
    name,
    target,
    info,
    completion_info,
    village
FROM unique_achievements