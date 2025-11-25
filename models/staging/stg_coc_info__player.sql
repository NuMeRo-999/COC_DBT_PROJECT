{{ config(
    materialized='incremental',
    on_schema_change= "sync_all_columns",
    tags=['silver','player']
) }}

SELECT
    player_id,
    clan_id,
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
FROM {{ ref('base_coc_info__player') }}

{% if is_incremental() %}
        WHERE ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }} )
    AND ingest_ts > (SELECT MAX(ingest_ts) FROM {{ this }})
{% endif %}