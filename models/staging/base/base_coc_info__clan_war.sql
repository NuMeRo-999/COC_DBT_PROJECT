{{ config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    tags=['silver','base','clan_war']
) }}

WITH current_war AS (
    SELECT
        MD5(clan_tag || '-' || COALESCE(raw:startTime::VARCHAR, 'current')) AS clan_war_id,
        clan_tag AS clan_id,
        raw:state::VARCHAR AS state,
        raw:opponent:tag::VARCHAR AS opponent_clan_tag,
        raw:opponent:name::VARCHAR AS opponent_clan_name,
        raw:teamSize::INT AS team_size,
        raw:attacksPerMember::INT AS attacks_per_member,
        raw:battleModifier::VARCHAR AS battle_modifier,
        raw:clan:badgeUrls AS badge_urls,
        raw:clan:clanLevel::INT AS clan_level,
        raw:clan:attacks::INT AS attacks,
        raw:clan:stars::INT AS stars,
        {{ coc_to_timestamp('raw:startTime') }} AS start_time,
        {{ coc_to_timestamp('raw:endTime') }} AS end_time,
        raw:clan:destructionPercentage::FLOAT AS destruction_percentage,
        COALESCE(raw:clan:expEarned::INT, 0) AS exp_earned,
        ingest_ts
    FROM {{ source('coc_raw_info', 'currentwar_raw') }}
    WHERE raw IS NOT NULL
),

war_log AS (
    SELECT
        MD5(clan_tag || '-' || raw:endTime::VARCHAR) AS clan_war_id,
        clan_tag AS clan_id,
        CASE 
            WHEN raw:result::VARCHAR = 'win' THEN 'win'
            WHEN raw:result::VARCHAR = 'lose' THEN 'lose'
            WHEN raw:result::VARCHAR = 'tie' THEN 'draw'
            ELSE 'unknown'
        END AS state,
        raw:opponent:tag::VARCHAR AS opponent_clan_tag,
        raw:opponent:name::VARCHAR AS opponent_clan_name,
        raw:teamSize::INT AS team_size,
        raw:attacksPerMember::INT AS attacks_per_member,
        'none' AS battle_modifier,
        raw:clan:badgeUrls:medium AS badge_url,
        raw:clan:clanLevel::INT AS clan_level,
        raw:clan:attacks::INT AS attacks,
        raw:clan:stars::INT AS stars,
        DATEADD(hour, -24, {{ coc_to_timestamp('raw:endTime') }}) AS start_time,
        {{ coc_to_timestamp('raw:endTime') }} AS end_time,
        raw:clan:destructionPercentage::FLOAT AS destruction_percentage,
        COALESCE(raw:clan:expEarned::INT, 0) AS exp_earned,
        ingest_ts
    FROM {{ source('coc_raw_info', 'warlog_raw') }}
    WHERE raw IS NOT NULL
),

{% if is_incremental() %}

	  WHERE ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }} )

{% endif %}

combined_wars AS (
    SELECT * FROM current_war
    UNION ALL
    SELECT * FROM war_log
)

SELECT
    clan_war_id,
    state,
    clan_id,
    opponent_clan_tag,
    opponent_clan_name,
    team_size,
    attacks_per_member,
    battle_modifier,
    badge_urls,
    clan_level,
    attacks,
    stars,
    start_time,
    end_time,
    destruction_percentage,
    exp_earned,
    CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP) AS ingest_ts
FROM combined_wars