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
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       description,
       merchant_id,
       product_id,
       reject_reason,
       rfq_request_id,
       sent,
       status,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
from {{ ref('scd2_mongo_rfq_response') }} t