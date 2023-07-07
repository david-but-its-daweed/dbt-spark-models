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
orders as (select o.order_id, merchant_order_id, id as product_id, dealId as deal_id, _id as order_product_id, user_id
    from (
        select distinct order_id, user_id
        from {{ ref('fact_order') }}
        where next_effective_ts_msk is null
    ) o
    left join (
        select distinct merchant_order_id, order_id from {{ ref('fact_merchant_order') }}
        where next_effective_ts_msk is null
    ) mo 
    on o.order_id = mo.order_id
    full join {{ ref('scd2_order_products_snapshot') }} op on op.merchOrdId = mo.merchant_order_id
    where op.dbt_valid_to is null
    ),

offer_product as (
select distinct
        customer_request_id, m.offer_id, m.user_id, product_id, deal_id, offer_product_id
    FROM (
        select customer_request_id, offer_id, deal_id, user_id
        from {{ ref('fact_customer_offers') }}
        ) m 
    JOIN 
        {{ ref('scd2_offer_products_snapshot') }} as o
        ON m.offer_id = o.offer_id
        where o.dbt_valid_to is null
    )
    
select distinct
    coalesce(op.deal_id, o.deal_id) as deal_id, 
    o.order_id as order_id,
    coalesce(op.user_id, o.user_id) as user_id, 
    merchant_order_id,
    customer_request_id,
    offer_id,
    coalesce(op.product_id, o.product_id) as product_id,
    order_product_id,
    offer_product_id
from offer_product op 
full join orders o on op.deal_id = o.deal_id and op.product_id = o.product_id
