{{
  config(
    materialized='table',
    alias='merchants',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_override_dataset_id': 'gold_migration',
    }
  )
}}

SELECT
    merchant_id,
    name AS merchant_name,
    origin_name,
    CAST(created_time / 1000 AS TIMESTAMP) AS created_datetime_utc,
    CAST(updated_time / 1000 AS TIMESTAMP) AS updated_datetime_utc,
    enabled AS is_enabled
FROM {{ source('mart', 'dim_merchant') }}
