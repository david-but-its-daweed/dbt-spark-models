{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true'
    }
) }}


WITH wide_data AS (
    SELECT _id AS customer_request_id,
           millis_to_ts_msk(ctms) AS date,
           dealId AS deal_id,
           explode(variants)
    FROM {{ source('mongo', 'b2b_core_customer_requests_daily_snapshot') }}
)


SELECT customer_request_id,
       deal_id,
       col.id AS sub_product_id,
       col.expectedQuantity as expectedQuantity,
       col.prices.ddpPerItem.amount as ddpPerItem,
       col.prices.ddpPerItem.ccy as ddpPerItem_ccy,
       col.prices.exwPerItem.amount as exwPerItem,
       col.prices.exwPerItem.ccy as exwPerItem_ccy,
       col.prices.taxBasePerItem.amount as taxBasePerItem,
       col.prices.taxBasePerItem.ccy as taxBasePerItem_ccy,
       col.prices.totalPerItem.amount as totalPerItem,
       col.prices.totalPerItem.ccy as totalPerItem_ccy,
       col.sampleDDPPrice.amount as sampleDDPPrice,
       col.sampleDDPPrice.ccy sampleDDPPrice_ccy,
       col.sampleType  as sample_type,
       col.box.l AS box_length,
       col.box.w AS box_width,
       col.box.h AS box_height,
       col.box.wt AS box_weight,
       col.box.q AS box_quantity,
       date
FROM wide_data
