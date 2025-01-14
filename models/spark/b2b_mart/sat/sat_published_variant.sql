{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}
SELECT
    variant_id,
    TIMESTAMP(created_ts_msk) AS created_ts_msk,
    enabled,
    gtin,
    hs_code,
    merchant_id,
    orig_colors,
    orig_main_image_url,
    product_id,
    TIMESTAMP(published_ts_msk) AS published_ts_msk,
    shipping_weight,
    sku,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_published_variant') }}
