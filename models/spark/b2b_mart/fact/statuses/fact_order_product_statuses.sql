{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}
  
WITH order_products AS (
    SELECT
        deal_id,
        user_id,
        customer_request_id,
        offer_id,
        offer_product_id,
        product_id,
        order_id,
        order_friendly_id,
        merchant_order_id,
        merchant_order_friendly_id,
        merchant_id,
        order_product_id,
        product_friendly_id,
        owner_email,
        owner_role
    FROM {{ ref('dim_deal_products') }}
),

statuses as
(SELECT
    merchant_order_id,
    product_id,
    min(CASE WHEN status = 'formingOrder' THEN event_ts_msk END) AS forming_order,
    min(CASE WHEN status = 'signingWithMerch' THEN event_ts_msk END) AS signing_with_merchant,
    min(CASE WHEN status = 'awaitingManufacturing' THEN event_ts_msk END) AS awaiting_manufacturing,
    min(CASE WHEN status = 'manufacturing' THEN event_ts_msk END) AS manufacturing,
    min(CASE WHEN status = 'psi' THEN event_ts_msk END) AS psi,
    min(CASE WHEN status = 'pickUp' THEN event_ts_msk END) AS pick_up,
    min(CASE WHEN status = 'orderCompleted' THEN event_ts_msk END) AS order_completed,
    min(CASE WHEN status = 'cancelled' THEN event_ts_msk END) AS cancelled,
    min(last_status) as last_status
FROM (
    SELECT
        merchant_order_id,
        product_id,
        status,
        event_ts_msk,
        first_value(status) over (partition by product_id order by event_ts_msk desc) as last_status
    FROM {{ ref('statuses_events') }}
    WHERE entity = 'product'
)
GROUP BY
    merchant_order_id,
    product_id
)

select 
  op.deal_id,
  op.user_id,
  op.customer_request_id,
  op.offer_id,
  op.offer_product_id,
  op.product_id,
  op.order_id,
  op.order_friendly_id,
  op.merchant_order_id,
  op.merchant_order_friendly_id,
  op.merchant_id,
  op.order_product_id,
  op.product_friendly_id,
  op.owner_email,
  op.owner_role,
  s.last_status,
  s.forming_order,
  s.signing_with_merchant,
  s.awaiting_manufacturing,
  s.manufacturing,
  s.psi,
  s.pick_up,
  s.order_completed,
  s.cancelled
from order_products op
join statuses s using (merchant_order_id, product_id)
