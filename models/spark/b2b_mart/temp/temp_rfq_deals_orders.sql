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

with rfq_sent as (
select 
null as deal_id,
rfq_request_id,
created_time,
sent_time,
is_top,
o.order_id,
o.user_id,
sent_status,
category_id,
max(variants.price.amount/1000000) as price_rfq,
max(variants.quantity) as amount_rfq,
sum(variants.price.amount/1000000*variants.quantity) as sum_price
from
(select 
r._id as rfq_request_id,
millis_to_ts_msk(r.ctms) as created_time,
millis_to_ts_msk(r.ctms) as sent_time,
isTop as is_top,
oid as order_id,
explode(productVariants) as variants,
status as sent_status,
categories[0] as category_id
from
{{ source('mongo', 'b2b_core_rfq_request_daily_snapshot') }} r
) r
left join (
    select distinct user_id, order_id from {{ ref('fact_order') }} where next_effective_ts_msk is null
    ) o on r.order_id = o.order_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9),

order_product as (select order_id, customer_request_id, offer_id, product_id, deal_id, 1 as order_product
    from {{ ref('dim_deal_products') }}
    where order_id is not null and product_id is not null
    ),

rfq_order as (
select 
type,
source,
campaign,
rfq_request_id,
created_time,
date(created_time) as created_date,
sent_time as rfq_sent_time,
millis_to_ts_msk(rr.ctms) as rfq_response_sent_time,
rr.sent as rfq_response_sent,
price_rfq,
amount_rfq,
sum_price,
is_top,
o.order_id,
op.customer_request_id,
op.offer_id,
o.deal_id,
o.user_id,
rr._id as rfq_response_id,
status,
mId as merchant_id,
pId as product_id,
coalesce(order_product, 0) as order_product,
null as offer_product,
case when converted = 1 and order_product = 1 and opp.product_id is not null then 1 else 0 end as converted,
rejectReason as reject_reason,
rejectReasonGroup as reject_reason_group,
sent_status,
category_id
from
rfq_sent o
left join {{ source('mongo', 'b2b_core_rfq_response_daily_snapshot') }} rr on o.rfq_request_id = rr.rfqid
left join (
    select distinct type, source, campaign, user_id 
    from {{ ref('fact_attribution_interaction') }}
    where last_interaction_type) a on o.user_id = a.user_id
left join order_product op on o.order_id = op.order_id and rr.pId = op.product_id
left join (select distinct order_id, 1 as converted from {{ ref('gmv_by_sources') }}) g on o.order_id = g.order_id
left join (select distinct order_id, product_id from {{ ref('order_product_prices') }}) opp on opp.order_id = g.order_id and opp.product_id = rr.pId
where created_time >= date('2022-06-01')
order by created_time desc
),

rfq_sent_deals as (
select 
rfq_request_id,
created_time,
sent_time,
is_top,
d.deal_id,
d.customer_request_id,
d.user_id,
sent_status,
category_id,
max(variants.price.amount/1000000) as price_rfq,
max(variants.quantity) as amount_rfq,
sum(variants.price.amount/1000000*variants.quantity) as sum_price
from
(select 
r._id as rfq_request_id,
millis_to_ts_msk(r.ctms) as created_time,
millis_to_ts_msk(r.stms) as sent_time,
isTop as is_top,
crid as customer_request_id,
explode(productVariants) as variants,
status as sent_status,
categories[0] as category_id
from
{{ source('mongo', 'b2b_core_customer_rfq_request_daily_snapshot') }} r
) r
left join (
    select customer_request_id, deal_id, user_id
    from {{ ref('fact_customer_requests') }}
    where next_effective_ts_msk is null
) d on r.customer_request_id = d.customer_request_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9),

offer_product as (
select distinct
        order_id, product_id, deal_id, customer_request_id, offer_id, product_id, 1 as offer_product, 0 as rfq_product
    from {{ ref('dim_deal_products') }}
    where deal_id is not null
    ),



rfq_deal as (
select 
type,
source,
campaign,
o.rfq_request_id,
created_time,
date(created_time) as created_date,
sent_time as rfq_sent_time,
millis_to_ts_msk(rr.ctms) as rfq_response_sent_time,
rr.sent as rfq_response_sent,
price_rfq,
amount_rfq,
sum_price,
is_top,
op.order_id,
o.customer_request_id,
offer_id,
o.deal_id,
o.user_id,
_id as rfq_response_id,
status,
rr.mId as merchant_id,
rr.pId as product_id,
case when g.order_id is not null then 1 else 0 end as order_product,
coalesce(order_product, 0) as offer_product,
coalesce(converted, 0) as converted,
rejectReason as reject_reason,
rejectReasonGroup as reject_reason_group,
sent_status,
category_id
from
rfq_sent_deals o
left join {{ ref('scd2_customer_rfq_response_snapshot') }} rr on o.rfq_request_id = rr.rfqid
left join (
    select distinct type, source, campaign, user_id 
    from {{ ref('fact_attribution_interaction') }}
    where last_interaction_type) a on o.user_id = a.user_id
left join offer_product op on o.customer_request_id = op.customer_request_id and rr.pId = op.product_id
left join (select distinct order_id, 1 as converted from {{ ref('gmv_by_sources') }}) g on op.order_id = g.order_id
where created_time >= date('2022-06-01')
order by created_time desc
)

select 
r.*,
level_1_category.name as level_1_category,
level_2_category.name as level_2_category,
level_1_category.id as cate_lv1_id,
level_2_category.id as cate_lv2_id,
case 
    when level_2_category is not null then 2
    when level_2_category is null and level_1_category is not null then 1
end as category_level,
merchant_name,
owner_id,
owner_email,
owner_role
from
(
select *,
max(order_product) over (partition by rfq_request_id) as order_product_request,
max(converted) over (partition by rfq_request_id) as converted_request,
count(rfq_response_id) over (partition by rfq_request_id) as responses
from rfq_order
union all 
select *,
max(order_product) over (partition by rfq_request_id) as order_product_request,
max(converted) over (partition by rfq_request_id) as converted_request,
count(rfq_response_id) over (partition by rfq_request_id) as responses
from rfq_deal
) r

left join (
    select distinct user_id,
    owner_id,
    owner_email,
    owner_role
    from {{ ref('fact_customers') }}
) c on r.user_id = c.user_id
    
left join (
    select distinct _id as merchant_id,
    coalesce(companyName, name) as merchant_name
    from {{ source('b2b_mart', 'dim_merchant') }}
    where next_effective_ts >= current_timestamp()
) m on r.merchant_id = m.merchant_id

left join (
    select
        category_id,
        name as category_name,
        level,
        level_1_category,
        level_2_category
    from {{ source('mart', 'category_levels') }}
) cat on r.category_id = cat.category_id
