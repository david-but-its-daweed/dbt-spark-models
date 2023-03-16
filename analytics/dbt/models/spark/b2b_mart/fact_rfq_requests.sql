{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'

    }
) }}
SELECT rfq_request_id,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       description,
       name,
       order_id,
       link,
       price,
       ccy,
       qty,
       status,
       TIMESTAMP(sent_ts_msk) AS sent_ts_msk,
       variants,
       top_rfq,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk,
       category_id, 
       category_name
FROM {{ ref('scd2_mongo_rfq_request') }} t
