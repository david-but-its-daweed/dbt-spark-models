{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@mkirusha',
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT
    product_id,
    min_price/1000000 as min_price,
    min_price_ccy,
    update_ts_msk,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_product_prices_daily_snapshot') }}
