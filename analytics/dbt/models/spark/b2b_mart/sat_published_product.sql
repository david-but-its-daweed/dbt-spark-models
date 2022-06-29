{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}
SELECT product_id,
       category_id,
       created_ts_msk,
       dangerous_kind,
       merchant_id,
       orig_description,
       orig_extra_image_urls,
       orig_main_image_url,
       orig_name,
       orig_url,
       sku,
       store_id,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_published_product') }} t