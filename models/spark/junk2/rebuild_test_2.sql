{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table'
) }}

select *, '2' as num2
from {{ ref('rebuild_test_1') }}
