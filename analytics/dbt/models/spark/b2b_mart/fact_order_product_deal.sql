{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH 
deals AS (
    SELECT DISTINCT
        user_id,
        deal_id,
        deal_type,
        estimated_date,
        interaction_id,
        estimated_gmv,
        owner_email,
        deal_name
    FROM {{ ref('fact_deals') }}
    WHERE partition_date_msk = (SELECT MAX(partition_date_msk) FROM {{ ref('fact_deals') }})
),

interactions as (
    SELECT DISTINCT 
        interaction_id,
        request_id,
        order_id,
        user_id,
        friendly_id as order_friendly_id,
        current_status as current_order_status,
        current_substatus as current_order_substatus,
        final_gmv,
        created_date,
        interaction_min_time as interaction_create_time,
        min_status_new_ts_msk,
        min_status_selling_ts_msk,
        min_status_manufacturing_ts_msk,
        min_status_shipping_ts_msk,
        min_status_cancelled_ts_msk,
        min_status_closed_ts_msk,
        min_status_claim_ts_msk,
        current_source as source,
        current_utm_campaign as utm_campaign,
        current_utm_source as utm_source,
        current_utm_medium as utm_medium,
        current_type as type,
        current_campaign as campaign,
        rn
    from {{ ref('fact_interactions') }}
),

orders as (
    select 
    order_id,
    last_order_status,
    last_order_sub_status,
    reject_reason,
    delivery_time_days
    from {{ ref('fact_order') }}
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

products as (
    select product_id, merchant_order_id, order_id, 
    deal_id,
    amount as product_price
    from {{ ref('order_product_prices') }}
),


psi as (select distinct
product_id,
merchant_order_id, 
psi_status_id,
psi_status
from {{ ref('fact_order_products') }}
),

admins as (
    select distinct admin_id,
    email, role
    from {{ ref('dim_user_admin') }}
),


order_owners as (
     select distinct o.order_id, user_id, owner_moderator_id as owner_id
     from
     (select distinct 
         order_id,
         owner_moderator_id,
         row_number() over (partition by order_id order by event_ts_msk desc) as rn
         from {{ ref('fact_order_change') }}
     ) o
     left join (select distinct user_id, order_id from {{ ref('fact_order') }}
     where next_effective_ts_msk is null) r on o.order_id = r.order_id
     where rn = 1

 ),

 user_owners as (
     select distinct user_id, owner_moderator_id as owner_id
     from
     (select distinct 
         user_id,
         owner_moderator_id,
         row_number() over (partition by user_id order by event_ts_msk desc) as rn
         from {{ ref('fact_user_change') }}
     ) o
     where rn = 1
 ),

owners as (
  select distinct user_id, order_id, owner_id, email as owner_email, role as owner_role
  from
  (
 select distinct 
 oo.user_id, oo.order_id, 
 coalesce(oo.owner_id, uo.owner_id) as owner_id
 from order_owners oo left join user_owners  uo on oo.user_id = uo.user_id
) o left join admins a 
     on o.owner_id = a.admin_id 
),

users as (
    select distinct user_id, fake
    from {{ ref('dim_user') }}
    where next_effective_ts_msk is null
),

offers as (
    select 
        op.offer_product_id,
        op.product_id,
        op.offer_id,
        op.trademark,
        op.hs_code,
        op.manufacturer_id,
        op.type as product_type,
        op.disabled,
        op.created_time_msk as order_product_created_time,
        co.customer_request_id,
        co.deal_id,
        co.offer_type,
        co.order_id,
        co.status as offer_status,
        co.user_id,
        co.created_time as offer_created_time
    from {{ ref('scd2_offer_products_snapshot') }} op
    left join {{ ref('fact_customer_offers') }} co on op.offer_id = co.offer_id
    where dbt_valid_to is null
)

select distinct
    coalesce(i.user_id, d.user_id) as user_id,
    p.deal_id,
    deal_type,
    i.interaction_id,
    request_id,
    i.order_id,
    order_friendly_id,
    mo.merchant_order_id,
    merchant_order_friendly_id,
    p.product_id,
    psi_status_id,
    psi_status,
    psi_status > 10 as psi_conducted,
    product_price,
    order_price_usd as merchant_order_price,
    firstmile_channel_id,
    manufacturing_days as declared_manufacturing_days,
    merchant_id,
    merchant_type,
    current_order_status,
    current_order_substatus,
    final_gmv,
    created_date,
    interaction_create_time,
    mo.created_ts_msk as merchant_order_created_ts_msk,
    min_status_new_ts_msk,
    min_status_selling_ts_msk,
    min_status_manufacturing_ts_msk,
    min_status_shipping_ts_msk,
    min_status_cancelled_ts_msk,
    min_status_closed_ts_msk,
    min_status_claim_ts_msk,
    type,
    source,
    campaign,
    utm_campaign,
    utm_source,
    utm_medium,
    estimated_date,
    estimated_gmv,
    oo.owner_email,
    oo.owner_role,
    deal_name,
    off.offer_product_id,
    off.offer_id,
    off.product_type,
    off.offer_type,
    off.offer_status,
    off.customer_request_id
from interactions i
left join orders o on i.order_id = o.order_id
left join merchant_order mo on mo.order_id = o.order_id
left join products p on p.merchant_order_id = mo.merchant_order_id
left join psi on p.merchant_order_id = psi.merchant_order_id and p.product_id = psi.product_id
left join deals d on p.deal_id = d.deal_id
left join owners oo on o.order_id = oo.order_id
left join offers off on off.deal_id = p.deal_id and off.product_id = p.product_id
join users u on coalesce(i.user_id, d.user_id) = u.user_id
where 
    (fake is null or not fake)
