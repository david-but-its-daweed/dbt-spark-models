{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-01-27', '2023-06-14', '2023-09-16']
    }
) }}



with price as (
    select min(least(prc[0], prc[1], prc[2])) as price, _id as product_id,
    case when max(least(prc[0], prc[1], prc[2]))/min(least(prc[0], prc[1], prc[2])) >= 2 then true else false end as suspicious
    from
    (select _id, prcQtyDis from {{ source('mongo', 'b2b_core_product_appendixes_daily_snapshot') }}
    where prcQtyDis[0] is not null) a
    left join 
    (select pId, prc from {{ source('mongo', 'b2b_core_variant_appendixes_daily_snapshot') }}
    where prc[0] is not null) b on _id = pId
    group by _id
),
    
expensive as (
    select
    product_id,
    max(price > price_standart) as expensive,
    max(suspicious) as suspicious
    from
    (
    select avg(price) over (partition by order_id, rfq_request_id) as price_standart,
    min(price) over (partition by order_id, rfq_request_id) as price_min,
    max(price) over (partition by order_id, rfq_request_id) as price_max,
    count(case when reject_reason is not null or response_status is not null then product_id end) over (partition by order_id, rfq_request_id) as reasons,
    price, suspicious, order_id, rfq_request_id, order_rfq_response_id, product_id
    from
    (select distinct 
    order_id, reject_reason, response_status, rfq_request_id, order_rfq_response_id, price, suspicious, p.product_id
    from {{ ref('rfq_metrics') }} r
    inner join price p on r.product_id = p.product_id
    where reject_reason not in (
        'totalyDifferent', 'functionalCharacteristics', 'doesntSuitRequirements', 'visualCharacteristics'
        ) and rfq_sent_ts_msk >= current_date() - interval 6 month
    )
    )
    where reasons > 1 and price_min != price_max
    group by product_id
),

products as
(    
    select product_id,
    avg(not_expensive) as expensive,
    max(suspicious) as suspicious
    from
(
select 
  e.product_id,
  case when 
      reject_reason in ('expensive', 'expensive (other options cheaper)',
            'higherThanMarketPrice', 'higherThanMarketPrice') then 0
    when not expensive then 1 
    when expensive then 0
   end as not_expensive,
   suspicious
from {{ ref('rfq_metrics') }} r
inner join expensive as e on r.product_id = e.product_id
)
where not_expensive is not null
group by product_id)

select 
product_id,
origName as name,
cate_lv1,
cate_lv2,
cate_lv3,
cate_lv4,
date('{{ var("start_date_ymd") }}') as partition_date_msk
from products p
left join (
select distinct _id, 
    origName,
    level_1_category['name'] as cate_lv1,
    level_2_category['name'] as cate_lv2,
    level_3_category['name'] as cate_lv3,
    level_4_category['name'] as cate_lv4
from {{ source('mongo', 'b2b_core_published_products_daily_snapshot') }} a
join {{ source('mart', 'category_levels') }} b  on a.categoryId = b.category_id) c on p.product_id = c._id
where expensive >= 0.7 and product_id is not null and not suspicious
and product_id in (
    select distinct product_id from {{ ref('sat_product_state') }}
    where status = 1)
