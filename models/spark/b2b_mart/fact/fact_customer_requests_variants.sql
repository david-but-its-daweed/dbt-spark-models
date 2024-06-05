{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true'
    }
) }}



WITH wide_data AS
  (SELECT _id AS customer_request_id,
          millis_to_ts_msk(ctms) AS date,
          dealId AS deal_id,
          explode(variants)
   FROM {{ source('mongo', 'b2b_core_customer_requests_daily_snapshot') }} )
SELECT customer_request_id,
       deal_id,
       col.id AS sub_product_id,
       col.expectedQuantity, date
FROM wide_data
