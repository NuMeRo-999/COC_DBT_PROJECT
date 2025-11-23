{{ config(
    materialized='table',
    tags=['silver','reference']
) }}

SELECT
    MD5('win') AS war_state_id,
    'win' AS war_state
UNION ALL
SELECT
    MD5('lose') AS war_state_id,
    'lose' AS war_state
UNION ALL
SELECT
    MD5('draw') AS war_state_id,
    'draw' AS war_state
UNION ALL
SELECT
    MD5('inWar') AS war_state_id,
    'inWar' AS war_state
UNION ALL
SELECT
    MD5('preparation') AS war_state_id,
    'preparation' AS war_state