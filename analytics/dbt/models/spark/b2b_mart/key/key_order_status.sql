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
5 AS id,
"New" AS status
UNION ALL
SELECT
10 AS id,
"Selling" AS status
UNION ALL
SELECT
20 AS id,
"Manufacturing" AS status
UNION ALL
SELECT
30 AS id,
"Shipping" AS status
UNION ALL
SELECT
40 AS id,
"Claim" AS status
UNION ALL
SELECT
50 AS id,
"Done" AS status
UNION ALL
SELECT
60 AS id,
"Cancelled" AS status
