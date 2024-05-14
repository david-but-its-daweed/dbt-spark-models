{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '4' as num4
from {{ ref('rebuild_test_2') }}
inner join (select device_id, num3 from {{ ref('rebuild_test_3') }} ) USING (device_id)
inner join {{ ref('rebuild_test_7') }} USING (device_id)
inner join {{ ref('rebuild_test_8') }} USING (device_id)
