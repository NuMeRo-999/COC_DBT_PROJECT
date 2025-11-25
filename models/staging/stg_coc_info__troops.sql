{{ config(
    tags=['silver','troops']
) }}

SELECT DISTINCT
    MD5(troop.value:name::VARCHAR || '-' || COALESCE(troop.value:village::VARCHAR, 'home')) AS troop_id,
    troop.value:name::VARCHAR AS name,
    troop.value:village::VARCHAR AS village
FROM {{ ref('base_coc_info__player') }},
LATERAL FLATTEN(input => raw_troops) AS troop
WHERE raw_troops IS NOT NULL
ORDER BY name