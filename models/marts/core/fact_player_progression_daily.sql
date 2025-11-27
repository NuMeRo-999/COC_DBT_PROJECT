{{ config(materialized='table', tags=['gold','facts','progression']) }}

WITH player_daily_snapshot AS (
    SELECT
        DATE_TRUNC('day', ingest_ts) as date_key,
        player_id,
        town_hall_level,
        exp_level,
        trophies,
        best_trophies,
        war_stars,
        donations,
        donations_received,
        clan_capital_contributions,
        ingest_ts
    FROM {{ ref('scd_coc_player_progression') }}
    WHERE dbt_valid_to IS NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY player_id, DATE_TRUNC('day', ingest_ts) 
        ORDER BY ingest_ts DESC
    ) = 1
),

daily_changes AS (
    SELECT
        pd.date_key,
        pd.player_id,
        pd.town_hall_level,
        pd.exp_level,
        pd.trophies,
        pd.war_stars,
        pd.donations,
        pd.donations_received,
        pd.clan_capital_contributions,
        LAG(pd.town_hall_level) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_town_hall,
        LAG(pd.exp_level) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_exp,
        LAG(pd.trophies) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_trophies,
        LAG(pd.war_stars) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_war_stars,
        LAG(pd.donations) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_donations,
        LAG(pd.donations_received) OVER (PARTITION BY pd.player_id ORDER BY pd.date_key) as prev_donations_received
    FROM player_daily_snapshot pd
)

SELECT
    dc.date_key,
    dc.player_id,
    dc.town_hall_level,
    dc.exp_level,
    dc.trophies,
    dc.war_stars,
    dc.town_hall_level - COALESCE(dc.prev_town_hall, dc.town_hall_level) as town_hall_change,
    dc.exp_level - COALESCE(dc.prev_exp, dc.exp_level) as exp_change,
    dc.trophies - dc.prev_trophies as trophies_change,
    dc.war_stars - COALESCE(dc.prev_war_stars, dc.war_stars) as war_stars_change,
    dc.donations - COALESCE(dc.prev_donations, 0) as donations_daily,
    CASE WHEN dc.town_hall_level > COALESCE(dc.prev_town_hall, 0) THEN 1 ELSE 0 END as town_hall_upgrade_flag,
    CASE WHEN dc.exp_level > COALESCE(dc.prev_exp, 0) THEN 1 ELSE 0 END as level_up_flag,
    (dc.donations - COALESCE(dc.prev_donations, 0)) - 
    (dc.donations_received - COALESCE(dc.prev_donations_received, 0)) as net_donations
FROM daily_changes dc