{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

select key as id, value as status
from (
    select explode(values)
    from {{ source('mongo', 'b2b_core_enumregistry_daily_snapshot') }}
    where key = 'offer.status'
    )
