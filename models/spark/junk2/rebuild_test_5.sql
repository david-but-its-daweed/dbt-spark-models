{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '5' as num5
from {{ ref('rebuild_test_4') }}
