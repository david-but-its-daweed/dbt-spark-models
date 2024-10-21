{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@mkirusha',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

SELECT
    key AS id,
    UPPER(SUBSTR(value, 0, 1)) || (SUBSTR(value, 2, LENGTH(value) - 1)) AS reject_reason
FROM (
    SELECT EXPLODE(values)
    FROM {{ source('mongo', 'b2b_core_enumregistry_daily_snapshot') }}
    WHERE key = 'issue.statusRejectReason'
)
