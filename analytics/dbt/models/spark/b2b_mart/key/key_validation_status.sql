{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT
    10 AS id,
    'initial' AS status
UNION ALL
SELECT
    20 AS id,
    'needsValidation' AS status
UNION ALL
SELECT
    25 AS id,
    'onHold' AS status
UNION ALL
SELECT
    30 AS id,
    'validated' AS status
UNION ALL
SELECT
    40 AS id,
    'rejected' AS status
