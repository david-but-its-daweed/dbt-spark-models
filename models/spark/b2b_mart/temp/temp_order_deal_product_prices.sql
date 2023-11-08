{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'

    }
) }}

select distinct
  d.deal_id,
  d.order_id,
  d.user_id,
  d.merchant_order_id,
  d.customer_request_id,
  d.offer_id,
  d.order_product_id,
  d.offer_product_id,
  gmv_initial,
  initial_gross_profit,
  final_gross_profit,
  type,
  source,
  campaign,
  retention,
  d.owner_email,
  d.owner_role,
  amount as price,
  coalesce(rfq.offer_type, offer.offer_type) as offer_type,
  row_number() over (partition by d.order_id order by amount desc) as order_rn,
  g.t as date_payed
from {{ ref('dim_deal_products') }} d
join {{ ref('gmv_by_sources') }}  g on d.order_id = g.order_id
join {{ ref('order_product_prices') }} p on p.product_id = d.product_id and d.order_id = p.order_id
left join (
  select
    offer_type, offer_id
  from {{ ref('fact_customer_offers') }}
) offer on d.offer_id = offer.offer_id
left join (
  select
    order_id, product_id, 'rfq' as offer_type
  from {{ ref('temp_rfq_deals_orders') }}
) rfq on d.order_id = rfq.order_id and d.product_id = rfq.product_id
