{{ config(
    tags=['silver','clan']
) }}

WITH clan_data AS (
    SELECT
        MD5(raw:tag::VARCHAR) AS clan_id,
        raw:tag::VARCHAR AS clan_tag,
        raw:name::VARCHAR AS name,
        raw:type::VARCHAR AS type,
        raw:description::VARCHAR AS description,
        raw:location:name::VARCHAR AS location,
        raw:isFamilyFriendly::BOOLEAN AS is_family_friendly,
        raw:badgeUrls:medium::VARCHAR AS badge_url,
        raw:clanLevel::INT AS clan_level,
        raw:clanPoints::INT AS clan_points,
        raw:requiredTrophies::INT AS required_trophies,
        raw:warFrequency::VARCHAR AS war_frequency,
        raw:warWinStreak::INT AS war_win_streak,
        raw:warWins::INT AS war_wins,
        raw:warTies::INT AS war_ties,
        raw:warLosses::INT AS war_losses,
        raw:isWarLogPublic::BOOLEAN AS is_war_log_public,
        raw:warLeague:name::VARCHAR AS war_league,
        raw:members::INT AS members,
        raw:memberList AS member_list,
        raw:labels AS labels,
        raw:requiredBuilderBaseTrophies::INT AS required_builder_base_trophies,
        raw:requiredTownhallLevel::INT AS required_townhall_level,
        raw:chatLanguage:name::VARCHAR AS chat_language,
        CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
    FROM {{ source('coc_raw_info', 'clan_raw') }}
    WHERE raw IS NOT NULL
)

SELECT
    clan_id,
    clan_tag,
    name,
    type,
    description,
    location,
    is_family_friendly,
    badge_url,
    clan_level,
    clan_points,
    required_trophies,
    war_frequency,
    war_win_streak,
    war_wins,
    war_ties,
    war_losses,
    is_war_log_public,
    war_league,
    members,
    required_builder_base_trophies,
    required_townhall_level,
    chat_language,
    ingest_ts
FROM clan_data