{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '9' as num9
from {{ ref('rebuild_test_5') }}
