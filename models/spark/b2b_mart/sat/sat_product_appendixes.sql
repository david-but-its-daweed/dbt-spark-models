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
       brand,
       quantity_first,
       quantity_second,
       quantity_third,
       is_top_product,
       supplier_link,
       supplier_product_link,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_product_appendixes') }} t