{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@ikhairullin',
      'team': 'onfy',
      'bigquery_load': 'false',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

SELECT
    oi.product_id,
    COUNT(DISTINCT oi.device_id) AS number_of_devices,
    SUM(oi.products_price) AS gmv,
    SUM(oi.products_price) / COUNT(DISTINCT oi.device_id) AS gmv_per_device
FROM {{ ref('orders_info') }} AS oi
WHERE DATE_TRUNC('day', oi.order_created_time_cet) > ADD_MONTHS(CURRENT_TIMESTAMP(), -3)
GROUP BY oi.product_id;