{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
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
is_top,
o.order_id,
o.user_id,
sum(variants.price.amount/1000000*variants.quantity) as sum_price
from
(select 
r._id as rfq_request_id,
millis_to_ts_msk(r.ctms) as created_time,
isTop as is_top,
oid as order_id,
explode(productVariants) as variants
from
{{ source('mongo', 'b2b_core_rfq_request_daily_snapshot') }} r
) r
left join (
    select distinct user_id, order_id from {{ ref('fact_order') }} where next_effective_ts_msk is null
    ) o on r.order_id = o.order_id
group by 1, 2, 3, 4, 5, 6),

order_product as (select order_id, product_id, deal_id, 1 as order_product
    from {{ ref('dim_deal_products') }}
    where order_id is not null and product_id is not null
    ),

rfq_order as (
select 
type,
source,
campaign,
rfq_request_id,
date(created_time) as created_date,
sum_price,
is_top,
o.order_id,
null as offer_id,
o.deal_id,
o.user_id,
rr._id as rfq_response_id,
status,
pId as product_id,
coalesce(order_product, 0) as order_product,
null as offer_product,
case when converted = 1 and order_product = 1 and opp.product_id is not null then 1 else 0 end as converted,
rejectReason as reject_reason,
rejectReasonGroup as reject_reason_group
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

orders as (select order_id, product_id, deal_id, 1 as offer_product
    from {{ ref('dim_deal_products') }}
    where deal_id is not null
    ),

rfq_sent_deals as (
select 
rfq_request_id,
created_time,
is_top,
d.deal_id,
d.customer_request_id,
d.user_id,
sum(variants.price.amount/1000000*variants.quantity) as sum_price
from
(select 
r._id as rfq_request_id,
millis_to_ts_msk(r.ctms) as created_time,
isTop as is_top,
crid as customer_request_id,
explode(productVariants) as variants
from
{{ source('mongo', 'b2b_core_customer_rfq_request_daily_snapshot') }} r
) r
left join (
    select customer_request_id, deal_id, user_id
    from {{ ref('fact_customer_requests') }}
) d on r.customer_request_id = d.customer_request_id
group by 1, 2, 3, 4, 5, 6),

offer_product as (
select distinct
        customer_request_id, o.offer_id, product_id, 1 as offer_product, 0 as rfq_product
    FROM {{ ref('scd2_offer_products_snapshot') }} as o
    JOIN 
        (
        select customer_request_id, offer_id from {{ ref('fact_customer_offers') }}
        ) m ON m.offer_id = o.offer_id
        where o.dbt_valid_to is null
    ),



rfq_deal as (
select 
type,
source,
campaign,
o.rfq_request_id,
date(created_time) as created_date,
sum_price,
is_top,
or.order_id,
offer_id,
o.deal_id,
o.user_id,
order_rfq_response_id as rfq_response_id,
status,
rr.product_id,
case when g.order_id is not null then 1 else 0 end as order_product,
coalesce(order_product, 0) as offer_product,
coalesce(converted, 0) as converted,
reject_reason as reject_reason,
reject_reason_group as reject_reason_group
from
rfq_sent_deals o
left join {{ ref('fact_customer_rfq_response') }} rr on o.rfq_request_id = rr.rfq_request_id
left join (
    select distinct type, source, campaign, user_id 
    from {{ ref('fact_attribution_interaction') }}
    where last_interaction_type) a on o.user_id = a.user_id
left join offer_product op on o.customer_request_id = op.customer_request_id and rr.product_id = op.product_id
left join orders or on or.deal_id = o.deal_id and rr.product_id = or.product_id
left join (select distinct order_id, 1 as converted from {{ ref('gmv_by_sources') }}) g on or.order_id = g.order_id
where created_time >= date('2022-06-01')
order by created_time desc
)

select 
r.*,
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
