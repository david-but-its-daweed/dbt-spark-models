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

)

select distinct
    i.deal_id,
    i.deal_friendly_id,
    coalesce(i.user_id, o.user_id) as user_id,
    cr.customer_request_id,
    off.offer_id,
    off.offer_product_id,
    coalesce(off.product_id, p.product_id) as product_id,
    o.order_id,
    o.order_friendly_id,
    mo.merchant_order_id,
    mo.merchant_order_friendly_id,
    mo.merchant_id,
    order_product_id,
    product_friendly_id,
    coalesce(i.owner_email, o.owner_email) as owner_email,
    coalesce(i.owner_role, o.owner_role) as owner_role
from deals i
left join customer_requests cr on cr.deal_id = i.deal_id
left join offers off on off.deal_id = i.deal_id and cr.customer_request_id = off.customer_request_id
full join orders o on i.order_id = o.order_id
left join merchant_order mo on mo.order_id = o.order_id
left join order_products p on p.merchant_order_id = mo.merchant_order_id
join users u on coalesce(i.user_id, o.user_id) = u.user_id
where 
    (fake is null or not fake)
    and (off.product_id = p.product_id or off.product_id is null or p.product_id is null)
