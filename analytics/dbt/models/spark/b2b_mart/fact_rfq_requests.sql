{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT rfq_request_id,
       created_ts_msk,
       description,
       name,
       order_id,
       link,
       price,
       ccy,
       qty,
       status,
       sent_ts_msk,
       variants,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_rfq_request') }} t