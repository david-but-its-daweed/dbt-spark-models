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
    ROUND(AVG(100.0 * (price_eur - discounted_price_eur) / price_eur), 1) AS mean_discount_percent,
    PERCENTILE(100.0 * (price_eur - discounted_price_eur) / price_eur, 0.5) AS median_discount_percent,
    PERCENTILE(100.0 * (price_eur - discounted_price_eur) / price_eur, 0.25) AS discount_percent_percent_percentile_25,
    PERCENTILE(100.0 * (price_eur - discounted_price_eur) / price_eur, 0.75) AS median_discount_percent_percentile_75
FROM pharmacy.feed_loaders_state
GROUP BY
    date,
    channel,
    cheapest_store_name,
    chosen_store_id
