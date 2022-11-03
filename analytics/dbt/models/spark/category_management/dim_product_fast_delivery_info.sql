{{ config(
    schema='category_management',
    materialized='view',
    meta = {
      'team': 'category_management',
    }
) }}
SELECT
  id,
  product_id,
  country,
  subdivision,
  TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_product_fast_delivery_info_daily_snapshot') }} t
