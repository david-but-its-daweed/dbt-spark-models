{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

-- SELECT device_id, count(*) as cn
-- FROM {{ source('mart', 'dim_device_min') }}
-- group by device_id
-- limit 100

SELECT *
FROM {{ ref('data_readiness_summary') }}
LIMIT 100

