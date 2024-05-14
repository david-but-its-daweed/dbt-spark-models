{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '3' as num3
from {{ ref('rebuild_test_1') }}
