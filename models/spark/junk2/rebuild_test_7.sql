{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '7' as num7
from {{ ref('rebuild_test_6') }}
