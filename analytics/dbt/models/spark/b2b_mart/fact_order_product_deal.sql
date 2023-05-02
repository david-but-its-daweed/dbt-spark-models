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
        deal_name,
        gmv_initial
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
        current_website_form as website_form,
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
1 as psi_conducted
from {{ ref('fact_psi') }}
),

admins as (
    select distinct admin_id,
    email, role
    from {{ ref('dim_user_admin') }}
),

order_owners as (
    select distinct order_id, owner_id, email as owner_email, role as owner_role
    from
    (select distinct 
        order_id,
        last_value(owner_moderator_id) over (partition by order_id order by event_ts_msk) as owner_id
        from {{ ref('fact_order_change') }}
    ) o
    left join admins a 
    on o.owner_id = a.admin_id 
),

user_owners as (
    select distinct user_id, owner_id, email as owner_email, role as owner_role
    from
    (select distinct 
        user_id,
        last_value(owner_moderator_id) over (partition by user_id order by event_ts_msk) as owner_id
        from {{ ref('fact_user_change') }}
    ) o
    left join admins a 
    on o.owner_id = a.admin_id 
),

users as (
    select distinct user_id, fake
    from {{ ref('dim_user') }}
    where next_effective_ts_msk is null
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
    psi_conducted,
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
    website_form,
    estimated_date,
    estimated_gmv,
    coalesce(oo.owner_email, uo.owner_email) as owner_email,
    coalesce(oo.owner_role, uo.owner_role) as owner_role,
    deal_name,
    gmv_initial,
    rn as order_rn
from interactions i
left join orders o on i.order_id = o.order_id
left join merchant_order mo on mo.order_id = o.order_id
left join products p on p.merchant_order_id = mo.merchant_order_id
left join psi on p.merchant_order_id = psi.merchant_order_id and p.product_id = psi.product_id
left join deals d on p.deal_id = d.deal_id
left join order_owners oo on o.order_id = oo.order_id
left join user_owners uo on i.user_id = uo.user_id
join users u on coalesce(i.user_id, d.user_id) = u.user_id
where 
    (fake is null or not fake)
