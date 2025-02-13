{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT key,
       CAST(id AS INT) AS id,
       name
FROM (
    SELECT key,
           EXPLODE(values) AS (id, name)
    FROM {{ source('mongo', 'b2b_core_enumregistry_daily_snapshot') }}
) AS m
ORDER BY 1, 2
