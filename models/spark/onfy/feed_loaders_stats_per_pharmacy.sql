{{ config(
    schema='onfy',
    materialized='table',
    file_format='delta',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_overwrite': 'true'
    }
) }}


SELECT
    date,
    channel,
    cheapest_store_name,
    chosen_store_id,
    COUNT(*) AS number_of_feeds,
    COUNT(DISTINCT product_id) AS number_of_products,
    AVG(discount_percent) AS mean_discount_percent,
    PERCENTILE(discount_percent, 0.5) AS median_discount_percent,
    PERCENTILE(discount_percent, 0.25) AS discount_percent_percent_percentile_25,
    PERCENTILE(discount_percent, 0.75) AS median_discount_percent_percentile_75
FROM {{ source('pharmacy', 'feed_loaders_state') }}
GROUP BY
    date,
    channel,
    cheapest_store_name,
    chosen_store_id
