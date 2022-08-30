{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'predictor_enabled': 'true',
      'anomalies_channel': '#aplotnikov-notifications',
      'anomalies_metric_name': 'Average Order Value',
    }
) }}
WITH orders AS (
    SELECT device_id,
        shipping_country AS country,
        os_type AS platform,
        gmv_initial,
        order_group_id,
        partition_date AS date
    FROM {{ source('mart', 'star_order_2020') }}
    ), 

     metric_agg AS (
    SELECT date as t,
        country,
        platform,
        SUM(COALESCE(gmv_initial, 0)) / COUNT(DISTINCT order_group_id) AS y
    FROM orders
    GROUP BY date, country, platform
    )

SELECT * FROM metric_agg
WHERE WHERE country IN ('RU', 'DE', 'FR', 'GB', 'IT', 'ES', 'CH', 'SE', 'IL', 'UA', 'MD', 'BY')
    AND platform in ('android', 'ios')
ORDER BY t, country, platform