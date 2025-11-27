{{ config(
    materialized='incremental',
    on_schema_change= "sync_all_columns",
    tags=['silver','player']
) }}

WITH ranked_players AS (
    SELECT
        player_id,
        clan_id,
        player_tag,
        name,
        town_hall_level,
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
        ingest_ts,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY ingest_ts DESC) as rn
    FROM {{ ref('base_coc_info__player') }}
)
SELECT
    player_id,
    clan_id,
    player_tag,
    name,
    town_hall_level,
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
    ingest_ts
FROM ranked_players
WHERE rn = 1
{% if is_incremental() %}
    AND ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }})
{% endif %}