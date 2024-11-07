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

with rfq_sent as (
select 
    payload.merchantId as merchant_id,
    payload.rfqRequestId as rfq_request_id,
    millis_to_ts_msk(payload.sentTime) as rfq_sent_time
from {{ source('b2b_mart', 'operational_events') }}
where type = 'rfqRequestSent'
),

rfq_rejected as (
    select 
        payload.merchantId as merchant_id,
        payload.rejectComment  as merchant_reject_comment,
        payload.rejectReason as merchant_reject_reason,
        millis_to_ts_msk(payload.rejectedTime) as merchant_rejected_time,
        payload.responseStatus as merchant_response_status,
        payload.rfqRequestId as rfq_request_id
from {{ source('b2b_mart', 'operational_events') }}
where type = 'rfqRequestRejected'

),

merchants as (
select 
    _id as merchant_id,
    companyName as company_name,
    name,
    publicContact.email,
   --- publicContact.kakaoTalk as kakao_talk,
    publicContact.person,
    publicContact.phone.code as phone_code,
    publicContact.phone.number as phone_number,
  ---  publicContact.weChat as we_chat,
  ---  publicContact.website as website
from {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }}
),


rfq_sent_deals as (
select 
rfq_request_id,
rfq_friendly_id,
rfq_name,
created_time,
is_top,
d.deal_id,
d.customer_request_id,
d.user_id,
d.country,
sent_status,
category_id,
max(variants.price.amount/1000000) as price_rfq,
max(variants.quantity) as amount_rfq,
sum(variants.price.amount/1000000*variants.quantity) as sum_price
from
(select 
r._id as rfq_request_id,
friendlyId as rfq_friendly_id,
millis_to_ts_msk(r.ctms) as created_time,
isTop as is_top,
crid as customer_request_id,
explode(productVariants) as variants,
status as sent_status,
name as rfq_name,
categories[0] as category_id
from {{ source('mongo', 'b2b_core_customer_rfq_request_daily_snapshot') }} r
) r
left join (
    select customer_request_id, deal_id, user_id, country
    from {{ ref('fact_customer_requests') }}
    where next_effective_ts_msk is null
) d on r.customer_request_id = d.customer_request_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11),


offer_product as (select order_id, customer_request_id, offer_id, product_id, deal_id, 
    case when order_id is not null then 1 else 0 end as order_product,
    case when product_id is not null then 1 else 0 end as offer_product

    from {{ ref('dim_deal_products') }}
    where deal_id is not null
    )
    
select *, 
    row_number() over (partition by rfq_request_id, merchant_id order by rfq_response_time) as merchant_response_number,
    count(rfq_response_id) over (partition by rfq_request_id, merchant_id) as merchant_responses    
from
(select distinct
    rfq_request_id,
    rfq_friendly_id,
    rfq_name,
    rfq_sent_time,
    merchant_id,
    company_name,
    name,
    email,
   --- kakao_talk,
    person,
    phone_code,
    phone_number,
   -- we_chat,
  --  website,
    merchant_response_status,
    merchant_rejected_time,
    merchant_reject_reason,
    merchant_reject_comment,
    created_time,
    is_top,
    o.deal_id,
    o.customer_request_id,
    user_id,
    o.country,
    sent_status,
    o.category_id,
    price_rfq,
    amount_rfq,
    sum_price as sum_price_rfq,
    op.order_id,
    offer_id,
    order_rfq_response_id as rfq_response_id,
    status,
    rr.product_id,
    rr.created_ts_msk as rfq_response_time,
    case when g.order_id is not null then 1 else 0 end as order_product,
    coalesce(order_product, 0) as offer_product,
    coalesce(converted, 0) as converted,
    reject_reason as reject_reason,
    reject_reason_group as reject_reason_group,
    level_1_category.name as level_1_category,
    level_2_category.name as level_2_category,
    level_1_category.id as cate_lv1_id,
    level_2_category.id as cate_lv2_id,
    case 
        when level_2_category is not null then 2
        when level_2_category is null and level_1_category is not null then 1
    end as category_level
from rfq_sent 
left join merchants using (merchant_id)
left join rfq_rejected using (merchant_id, rfq_request_id)
left join rfq_sent_deals o using (rfq_request_id)
left join (
    select distinct * from {{ ref('fact_customer_rfq_responses') }}
    where next_effective_ts_msk is null
) rr using (rfq_request_id, merchant_id)
left join offer_product op on o.customer_request_id = op.customer_request_id and rr.product_id = op.product_id
left join (select distinct order_id, 1 as converted from b2b_mart.gmv_by_sources) g on op.order_id = g.order_id
left join (
    select
        category_id,
        name as category_name,
        level,
        level_1_category,
        level_2_category
    from {{ source('mart', 'category_levels') }}
) cat on o.category_id = cat.category_id
where created_time >= date('2023-08-01')
order by created_time desc
)
