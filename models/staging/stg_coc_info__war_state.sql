SELECT
    war_state_id,
    war_state
FROM {{ ref('war_states') }}