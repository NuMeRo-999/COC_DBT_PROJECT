SELECT
    state_id,
    state
FROM {{ ref('war_states') }}