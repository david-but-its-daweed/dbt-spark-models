{{
  config(
    materialized='table',
    alias='merchants',
    schema='gold',
    meta = {
        'model_owner' : '@gusev'
    }
  )
}}

SELECT
    merchant_id,
    name AS merchant_name,
    origin_name,
    DATETIME(TIMESTAMP_MILLIS(created_time)) AS created_datetime_utc,
    DATETIME_TRUNC(DATETIME(TIMESTAMP_MILLIS(updated_time)), SECOND) AS updated_datetime_utc,
    enabled AS is_enabled
FROM {{ source('mart', 'dim_merchant') }}
