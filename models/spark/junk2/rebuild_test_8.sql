{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select device_id, '8' as num8
from (
select distinct device_id
from {{ source('mart', 'dim_device_min') }}
)
order by device_id
limit 100
