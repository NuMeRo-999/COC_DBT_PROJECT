{{ config(
    materialized='incremental',
) }}

WITH player_data AS (
    SELECT
        MD5(raw:tag::VARCHAR) AS player_id,
        MD5(raw:clan:tag::VARCHAR) AS clan_id,
        raw:name::VARCHAR AS name,
        raw:townHallLevel::INT AS town_hall_level,
        raw:townHallWeaponLevel::INT AS town_hall_weapon_level,
        raw:expLevel::INT AS exp_level,
        raw:trophies::INT AS trophies,
        raw:bestTrophies::INT AS best_trophies,
        raw:warStars::INT AS war_stars,
        raw:attackWins::INT AS attack_wins,
        raw:defenseWins::INT AS defense_wins,
        raw:role::VARCHAR AS role,
        raw:warPreference::VARCHAR AS war_preference,
        raw:donations::INT AS donations,
        raw:donationsReceived::INT AS donations_received,
        raw:clanCapitalContributions::BIGINT AS clan_capital_contributions,
        TRY_TO_TIMESTAMP(raw:clan:joinedClanAt::VARCHAR) AS joined_clan_at,
        TRY_TO_TIMESTAMP(raw:leftClanAt::VARCHAR) AS left_clan_at,
        raw:achievements AS achievements,
        raw:heroes AS heroes,
        raw:heroEquipment AS hero_equipment,
        raw:troops AS troops,
        raw:spells AS spells,
        CONVERT_TIMEZONE('UTC', current_date()) AS ingest_ts
    FROM {{ source('coc_raw_info', 'player_raw') }}
    WHERE raw IS NOT NULL
)

SELECT
    player_id,
    clan_id,
    name,
    town_hall_level,
    town_hall_weapon_level,
    exp_level,
    trophies,
    best_trophies,
    war_stars,
    attack_wins,
    defense_wins,
    role,
    war_preference,
    donations,
    donations_received,
    clan_capital_contributions,
    joined_clan_at,
    left_clan_at,
    ingest_ts
FROM player_data