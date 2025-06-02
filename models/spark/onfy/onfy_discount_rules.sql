{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

SELECT     
    product_id,
    null as store_id,
    channel,
    weight,
    pessimization_type,
    discount_percent,
    source,
    comment
FROM {{ source('onfy', 'auto_discount_rules') }}
UNION ALL
SELECT *
FROM {{ ref('onfy_discount_rules_manual') }}
