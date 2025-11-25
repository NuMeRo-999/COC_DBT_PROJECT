
SELECT
    clan_war_id,
    state_id,
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
    ingest_ts
FROM {{ ref('base_coc_info__clan_war') }}