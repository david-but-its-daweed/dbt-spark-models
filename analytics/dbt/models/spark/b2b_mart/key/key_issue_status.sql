{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select key as id, upper(substr(value, 0, 1))||(substr(value, 2, length(value) - 1 )) as status
from (
    select explode(values)
    from {{ source('mongo', 'b2b_core_enumregistry_daily_snapshot') }}
    where key = 'issue.status'
    )
