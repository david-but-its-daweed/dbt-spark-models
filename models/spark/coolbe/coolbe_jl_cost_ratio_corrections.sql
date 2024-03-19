{{ config(
    schema='coolbe',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'clan',
      'bigquery_load': 'true'
    }
) }}

WITH stg AS (
    SELECT
        shipping_country,
        correction,
        CAST(partition_date AS DATE) AS partition_date,
        CAST(snapshot_date AS DATE) AS snapshot_date,
        MAX(CAST(snapshot_date AS DATE)) OVER (PARTITION BY partition_date, shipping_country) AS last_snapshot_date
    FROM {{ source('goods', 'coolbe_jl_cost_ratio_corrections') }}
)

SELECT
    shipping_country,
    correction,
    partition_date,
    snapshot_date
FROM stg
WHERE last_snapshot_date = snapshot_date