{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT order_rfq_response_id,
       comment,
       created_ts_msk,
       description,
       merchant_id,
       product_id,
       reject_reason,
       rfq_request_id,
       sent,
       status,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_rfq_response') }} t