{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'predictor_enabled': 'true',
      'anomalies_channel': '#aplotnikov-notifications',
      'anomalies_metric_name': 'Number of SKU per Buying Device',
    }
) }}
WITH orders AS (
    SELECT device_id,
        order_id,
        order_group_id,
        product_quantity AS quantity,
        shipping_country AS country,
        LOWER(os_type) AS platform,
        partition_date AS date
    FROM {{ source('mart', 'star_order_2020') }}
    ),

     orders_grouped AS (
    SELECT device_id,
        date,
        country,
        platform,
        COUNT(order_id) AS n_goods,
        COUNT(DISTINCT order_group_id) AS n_orders,
        SUM(quantity) AS n_items
    FROM orders
    GROUP BY device_id, date, country, platform)

SELECT date as t,
    country,
    platform,
    AVG(n_goods) AS y
FROM orders_grouped
WHERE WHERE country IN ('RU', 'DE', 'FR', 'GB', 'IT', 'ES', 'CH', 'SE', 'IL', 'UA', 'MD', 'BY')
    AND platform in ('android', 'ios')
GROUP BY t, country, platform