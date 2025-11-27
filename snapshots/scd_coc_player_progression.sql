{% snapshot scd_coc_player_progression %}

{{
    config(
        target_schema='coc_snapshots',
        unique_key='player_id',
        strategy='timestamp',
        updated_at='ingest_ts',
        hard_deletes='invalidate'
    )
}}

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
{% endsnapshot %}