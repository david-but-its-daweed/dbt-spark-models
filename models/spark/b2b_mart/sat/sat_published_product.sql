{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}
SELECT product_id,
       category_id,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       dangerous_kind,
       merchant_id,
       orig_description,
       orig_extra_image_urls,
       orig_main_image_url,
       orig_name,
       orig_url,
       sku,
       store_id,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_published_product') }} t