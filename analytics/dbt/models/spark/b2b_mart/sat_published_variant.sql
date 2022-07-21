{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}
SELECT variant_id,
       created_ts_msk,
       enabled,
       gtin,
       hs_code,
       merchant_id,
       orig_colors,
       orig_main_image_url,
       product_id,
       public,
       published_ts_msk,
       shipping_weight,
       sku,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_published_variant') }}