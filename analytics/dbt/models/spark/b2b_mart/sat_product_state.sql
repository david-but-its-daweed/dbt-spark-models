{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics'
    }
) }}
SELECT product_id,
       reject_reason,
       status,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_product_state') }} t