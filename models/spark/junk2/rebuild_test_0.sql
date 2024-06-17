{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table',
    file_format='delta'
) }}

select device_id, '0' as num0
from (
select distinct device_id
from {{ source('mart', 'dim_device_min') }}
)
order by device_id
limit 100
