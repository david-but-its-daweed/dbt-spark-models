{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


select distinct
c.customer_request_id,
c.category_id,
c.category_name,
c.created_time as request_created_time,
c.deal_id,
c.price_type,
c.status as request_status,
c.reject_reason,
c.user_id,
o.offer_id,
o.merchant_id,
o.offer_type,
o.order_id,
o.status as offer_status,
op.offer_product_id,
op.product_id,
op.trademark,
op.manufacturer_id,
op.disabled,
op.type as product_type
from b2b_mart.fact_customer_requests c
left join b2b_mart.fact_customer_offers o on c.customer_request_id = o.customer_request_id
left join b2b_mart.scd2_offer_products_snapshot op on o.offer_id = op.offer_id
