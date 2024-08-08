{{
  config(
    materialized='incremental',
    file_format='parquet',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

SELECT
    partition_date,
    product_variant_id,
    product_id,
    logistics_product_id,
    number_of_reserved,
    number_of_products_in_stock,
    stock_update_ts,
    brand_name,
    product_name,
    product_url,
    product_dimensions,
    merchant_id,
    original_merchant_price,
    product_weight
FROM {{ ref('fbj_product_stocks_current') }}