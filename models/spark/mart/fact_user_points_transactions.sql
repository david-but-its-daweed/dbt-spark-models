{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'date_msk',
      'bigquery_upload_horizon_days': '14',
      'priority_weight': '150'
    }
) }}

SELECT
    _id AS id,
    userid AS user_id,
    kind,
    type,
    CAST((effectiveusd / 1000000) AS DECIMAL(38, 6)) AS effective_usd,
    hidden,
    refid AS refid,
    TO_DATE(CAST(CAST(CONV(SUBSTRING(_id, 0, 8), 16, 10) AS BIGINT) + 10800 AS TIMESTAMP)) AS date_msk,
    amount,
    CAST(NULL AS BOOLEAN) AS bad_order_id,
    index,
    pending
FROM {{ source('mongo', 'points_points_transactions_daily_snapshot') }}
WHERE TO_DATE(CAST(CAST(CONV(SUBSTRING(_id, 0, 8), 16, 10) AS BIGINT) + 10800 AS TIMESTAMP)) < TO_DATE(NOW())

