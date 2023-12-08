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
    
WITH 
deals AS (
    SELECT DISTINCT
        user_id,
        deal_id,
        issue_friendly_id as deal_friendly_id,
        deal_type,
        estimated_date,
        estimated_gmv,
        owner_email,
        owner_role,
        deal_name,
        order_id
    FROM {{ ref('fact_deals') }}
    WHERE next_effective_ts_msk IS NULL
),


orders as (
    select 
    order_id,
    friendly_id as order_friendly_id,
    last_order_status,
    last_order_sub_status,
    reject_reason,
    delivery_time_days,
    owner_id,
    user_id,
    email as owner_email,
    role as owner_role
    from {{ ref('fact_order') }}
    left join 
    (
        select distinct
            admin_id,
            email,
            role
        from {{ ref('dim_user_admin') }}
     ) o on owner_id = admin_id
    where next_effective_ts_msk is null
),

merchant_order as (
    select 
    order_id,
    merchant_order_id,
    created_ts_msk,
    currency,
    order_price_usd,
    firstmile_channel_id,
    friendly_id as merchant_order_friendly_id,
    manufacturing_days,
    merchant_id,
    merchant_type
    from {{ ref('fact_merchant_order') }}
    where next_effective_ts_msk is null
),

order_products as (
    select
      order_product_id,
      product_id,
      deal_id,
      product_friendly_id,
      hs_code,
      merchant_order_id
    from {{ ref('fact_order_products') }}
),

users as (
    select distinct user_id, fake
    from {{ ref('dim_user') }}
    where next_effective_ts_msk is null
),

offers as (
    select 
        offer_product_id,
        product_id,
        offer_id,
        trademark,
        hs_code,
        manufacturer_id,
        product_type,
        disabled,
        offer_product_created_time,
        customer_request_id,
        deal_id,
        offer_type,
        order_id,
        offer_status,
        user_id,
        offer_created_time
    from {{ ref('fact_offer_product') }} op
    where (not disabled or disabled is null)
    AND next_effective_ts_msk IS NULL
),

customer_requests as (
    select
        customer_request_id,
        deal_id
    from {{ ref('fact_customer_requests') }}
    WHERE next_effective_ts_msk IS NULL

),

deals_data as (
  select distinct
    i.deal_id,
    i.order_id,
    i.deal_friendly_id,
    i.user_id,
    cr.customer_request_id,
    off.offer_id,
    off.offer_product_id,
    off.product_id,
    coalesce(i.owner_email) as owner_email,
    coalesce(i.owner_role) as owner_role
from deals i
left join customer_requests cr on cr.deal_id = i.deal_id
left join offers off on off.deal_id = i.deal_id and cr.customer_request_id = off.customer_request_id
where i.deal_id is not null
),

orders_data as (
  select distinct
    o.user_id,
    deals.deal_id,
    p.product_id,
    o.order_id,
    o.order_friendly_id,
    mo.merchant_order_id,
    mo.merchant_order_friendly_id,
    mo.merchant_id,
    order_product_id,
    product_friendly_id,
    o.owner_email as owner_email,
    o.owner_role as owner_role
from orders o
left join merchant_order mo on mo.order_id = o.order_id
left join order_products p on p.merchant_order_id = mo.merchant_order_id
left join deals on o.order_id = deals.order_id
where o.order_id is not null
),

products_list as (
select distinct *
from
(
select distinct
  deal_id,
  product_id,
  offer_product_id as order_offer_product_id
from deals_data
union all
select distinct
  deal_id,
  product_id,
  order_product_id as order_offer_product_id
from orders_data
where deal_id is not null
)
)


select distinct
  products_list.deal_id,
  deals_data.deal_friendly_id,
  coalesce(deals_data.user_id, orders_data.user_id) as user_id,
  deals_data.customer_request_id,
  deals_data.offer_id,
  deals_data.offer_product_id,
  products_list.product_id,
  orders_data.order_id,
  orders_data.order_friendly_id,
  orders_data.merchant_order_id,
  orders_data.merchant_order_friendly_id,
  orders_data.merchant_id,
  orders_data.order_product_id,
  orders_data.product_friendly_id,
  coalesce(deals_data.owner_email, orders_data.owner_email) as owner_email,
  coalesce(deals_data.owner_role, orders_data.owner_role) as owner_role

from products_list
left join deals_data on products_list.deal_id = deals_data.deal_id
  and coalesce(products_list.product_id, '') = coalesce(deals_data.product_id, '')
  and coalesce(products_list.order_offer_product_id, '') = coalesce(deals_data.offer_product_id, '')
left join orders_data on products_list.deal_id = orders_data.deal_id
  and coalesce(products_list.product_id, '') = coalesce(orders_data.product_id, '')
  and coalesce(products_list.order_offer_product_id, '') = coalesce(orders_data.order_product_id, '')
join users u on coalesce(deals_data.user_id, orders_data.user_id) = u.user_id
where (fake is null or not fake)

union all
select distinct
  '' as deal_id,
  '' as deal_friendly_id,
  orders_data.user_id,
  '' as customer_request_id,
  '' as offer_id,
  '' as offer_product_id,
  orders_data.product_id,
  orders_data.order_id,
  orders_data.order_friendly_id,
  orders_data.merchant_order_id,
  orders_data.merchant_order_friendly_id,
  orders_data.merchant_id,
  orders_data.order_product_id,
  orders_data.product_friendly_id,
  orders_data.owner_email,
  orders_data.owner_role

from orders_data 
join users u on orders_data.user_id = u.user_id
where (fake is null or not fake) and deal_id is null
