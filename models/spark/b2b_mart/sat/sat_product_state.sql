{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@ekutynina',
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT product_id,
       reject_reason,
       status,
       TIMESTAMP(dbt_valid_from) as effective_ts_msk,
       TIMESTAMP(dbt_valid_to) as next_effective_ts_msk
FROM {{ ref('scd2_mongo_product_state') }} t