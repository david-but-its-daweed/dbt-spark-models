{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

SELECT
    product_id,
    null AS store_id,
    channel,
    weight,
    pessimization_type,
    discount_percent,
    source,
    comment
FROM {{ ref('auto_discount_rules') }}
UNION ALL
SELECT *
FROM {{ ref('onfy_discount_rules_manual') }}
