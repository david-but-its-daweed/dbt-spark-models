{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

with rfq_sent_deals as (
  select 
    rfq_request_id,
    created_time,
    is_top,
    d.deal_id,
    d.customer_request_id,
    d.user_id,
    sent_status,
    category_id,
    rfq_friendly_id,
    max(variants.price.amount/1000000) as price_rfq,
    max(variants.quantity) as amount_rfq,
    sum(variants.price.amount/1000000*variants.quantity) as sum_price
  from (
    select 
      r._id as rfq_request_id,
      millis_to_ts_msk(r.ctms) as created_time,
      isTop as is_top,
      crid as customer_request_id,
      explode(productVariants) as variants,
      status as sent_status,
      categories[0] as category_id,
      friendlyId as rfq_friendly_id
    from {{ ref('scd2_customer_rfq_request_snapshot') }} r
    where dbt_valid_to is null and isDeleted is null
) r
left join (
    select customer_request_id, deal_id, user_id
    from {{ ref('fact_customer_requests') }}
    WHERE next_effective_ts_msk IS NULL
) d on r.customer_request_id = d.customer_request_id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9),


offer_product as (
  select distinct
        order_id,
        merchant_order_id,
        product_id,
        deal_id,
        customer_request_id,
        offer_id,
        product_id,
        1 as offer_product,
        0 as rfq_product
    from {{ ref('dim_deal_products') }}
    where deal_id is not null
),


rfq_deal as (
  select 
    type,
    source,
    campaign,
    o.rfq_request_id,
    date(created_time) as created_date,
    price_rfq,
    amount_rfq,
    sum_price,
    is_top,
    op.order_id,
    o.customer_request_id,
    offer_id,
    o.deal_id,
    o.user_id,
    order_rfq_response_id as rfq_response_id,
    status,
    rr.merchant_id,
    merchant_order_id,
    rr.product_id,
    case when g.order_id is not null then 1 else 0 end as order_product,
    coalesce(order_product, 0) as offer_product,
    coalesce(converted, 0) as converted,
    reject_reason as reject_reason,
    reject_reason_group as reject_reason_group,
    sent_status,
    category_id
  from rfq_sent_deals o
  left join (
    select * from {{ ref('fact_customer_rfq_responses') }}
    where next_effective_ts_msk is null
    ) rr on o.rfq_request_id = rr.rfq_request_id
  left join (
    select distinct
      type,
      source,
      campaign,
      user_id
    from {{ ref('fact_attribution_interaction') }}
    where last_interaction_type
  ) a on o.user_id = a.user_id
  left join offer_product op on o.customer_request_id = op.customer_request_id and rr.product_id = op.product_id
  left join (select distinct order_id, 1 as converted from {{ ref('gmv_by_sources') }}) g on op.order_id = g.order_id
  where created_time >= date('2022-06-01')
  order by created_time desc
)

select 
  r.*,
  actual_price,
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
from (
  select *,
    max(order_product) over (partition by rfq_request_id) as order_product_request,
    max(converted) over (partition by rfq_request_id) as converted_request,
    count(rfq_response_id) over (partition by rfq_request_id) as responses
  from rfq_deal
) r
left join (
    select distinct
      user_id,
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
    from {{source('mart', 'category_levels') }}
) cat on r.category_id = cat.category_id

left join (
    select 
        product_id, 
        merchant_order_id,
        amount as actual_price
    from {{ ref('order_product_prices') }}
) p on p.product_id = r.product_id and p.merchant_order_id = r.merchant_order_id
