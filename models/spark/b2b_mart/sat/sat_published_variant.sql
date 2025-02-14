{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true'
    }
) }}


SELECT _id AS variant_id,
       TIMESTAMP(MILLIS_TO_TS_MSK(createdTimeMs)) AS created_ts_msk,
       enabled,
       gtin,
       hsCode AS hs_code,
       merchantId AS merchant_id,
       origColors AS orig_colors,
       origMainImageUrl AS orig_main_image_url,
       productid AS product_id,
       TIMESTAMP(MILLIS_TO_TS_MSK(createdTimeMs)) AS published_ts_msk,
       shippingWeight AS shipping_weight,
       sku,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_published_variants_snapshot') }}
