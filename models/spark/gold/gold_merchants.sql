{{
  config(
    materialized='table',
    alias='merchants',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev'
    }
  )
}}

SELECT
    merchant_id,
    name AS merchant_name,
    origin_name,
    cast(created_time / 1000 AS TIMESTAMP) AS created_datetime_utc,
    cast(updated_time / 1000 AS TIMESTAMP) AS updated_datetime_utc,
    enabled AS is_enabled
FROM {{ source('mart', 'dim_merchant') }}
