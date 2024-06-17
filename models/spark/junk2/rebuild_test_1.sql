{{ config(
    meta = {
      'model_owner' : '@skotlov'
    },
    schema='skotlov',
    materialized='table',
    file_format='delta'
) }}

select *, '1' as num1
from {{ ref('rebuild_test_0') }}
