{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}
  
with 
orders as (select order_id, id as product_id, dealId as deal_id, _id as order_product_id
    from {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }} op
    left join (
        select distinct merchant_order_id, order_id from {{ ref('fact_merchant_order') }}
        where next_effective_ts_msk is null
    ) mo 
    on op.merchOrdId = mo.merchant_order_id
    where merchOrdId is not null and dealId is not null),

offer_product as (
select distinct
        customer_request_id, offer_id, id as product_id, deal_id, _id as offer_product_id
    FROM {{ source('mongo', 'b2b_core_offer_products_daily_snapshot') }} as o
    JOIN 
        (
        select customer_request_id, offer_id, deal_id from {{ ref('fact_customer_offers') }}
        ) m ON m.offer_id = o.offerId
    )
    
select 
    coalesce(op.deal_id, o.deal_id) as deal_id, 
    o.order_id as order_id,
    customer_request_id,
    offer_id,
    coalesce(op.product_id, o.product_id) as product_id,
    order_product_id,
    offer_product_id
from offer_product op 
full join orders o on op.deal_id = o.deal_id and op.product_id = o.product_id
