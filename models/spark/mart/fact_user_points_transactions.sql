{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@general_analytics',
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
    cast((effectiveusd / 1000000) AS DECIMAL(38, 6)) AS effective_usd,
    hidden,
    refid AS refid,
    to_date(cast(cast(conv(substring(_id, 0, 8), 16, 10) AS BIGINT) + 10800 AS TIMESTAMP)) AS date_msk,
    amount,
    cast(NULL AS BOOLEAN) AS bad_order_id,
    index,
    pending
FROM {{ source('mongo', 'points_points_transactions_daily_snapshot') }}
WHERE to_date(cast(cast(conv(substring(_id, 0, 8), 16, 10) AS BIGINT) + 10800 AS TIMESTAMP)) < to_date(now())

