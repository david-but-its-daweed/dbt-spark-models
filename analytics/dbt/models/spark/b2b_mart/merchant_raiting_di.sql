{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk'
    }
) }}

with price as (
    select min(least(prc[0], prc[1], prc[2])) as price, _id as product_id
    from
    (select _id, prcQtyDis from {{ source('mongo', 'b2b_core_product_appendixes_daily_snapshot') }}
    where prcQtyDis[0] is not null) a
    left join 
    (select pId, prc from {{ source('mongo', 'b2b_core_variant_appendixes_daily_snapshot') }}
    where prc[0] is not null) b on _id = pId
    group by _id
),
    
expensive as (
    select distinct 
    order_rfq_response_id as id, 
    price > price_standart as expensive,
    price_min != price_max as different_prices
    from
    (
    select avg(price) over (partition by order_id, rfq_request_id) as price_standart,
    min(price) over (partition by order_id, rfq_request_id) as price_min,
    max(price) over (partition by order_id, rfq_request_id) as price_max,
    price, order_id, rfq_request_id, order_rfq_response_id
    from
    (select distinct order_id, rfq_request_id, order_rfq_response_id, price
    from {{ ref('rfq_metrics') }} r
    left join price p on r.product_id = p.product_id
    )
    )
),
    
rfq_stat as
(    
select 
  merchant_id,
  count(distinct order_rfq_response_id) as rfq_answers,
  count(distinct rfq_request_id) as rfqs,
  AVG(time_rfq_response) as time,
  AVG(CASE WHEN
      date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "H") >= 8
      AND date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "H") <= 21
      AND NOT(
        date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "MM-dd") >= "12-30"
        AND date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "MM-dd") <= "01-08")
      AND NOT(date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "MM-dd") >= "01-30"
        AND date_format(timestamp(rfq_sent_ts_msk) + INTERVAL 5 HOUR, "MM-dd") <= "02-08")
    THEN time_rfq_response
  END) AS time_corrected,
  count(distinct case when response_status in ('accepted', 'rejected') then order_rfq_response_id end) as responsed,
  count(
      distinct case when response_status in ('accepted', 'rejected') and different_prices then order_rfq_response_id end
      ) as responsed_with_prices,
  count(distinct case when reject_reason 
      in ('totalyDifferent', 'littleDifferent', 'termsNotSuit', 'incorrectPackaging') 
      then order_rfq_response_id end) as rfq_incorrect,
  count(distinct case when reject_reason 
      in ('expensive') and different_prices
      then order_rfq_response_id end) as rfq_expensive,
  count(distinct case when converted = 1 then order_id end) as rfq_orders_converted,
  count(distinct case when converted = 1 then order_rfq_response_id end) as rfq_answer_converted,
  count(distinct rfq_request_id) >= 4 as valid_merchant,
  count(distinct case when (reject_reason is null or reject_reason not
      in ('totalyDifferent', 'littleDifferent', 'termsNotSuit', 'incorrectPackaging') )
      and category_id > '' then category_id end) as rfq_categories,
  count(distinct product_id) as rfq_products,
  count(distinct case when documents_attached then order_rfq_response_id end) as rfq_with_attached_docks,
  avg(manufacturingDays) as rfq_manufacturing_days
from {{ ref('rfq_metrics') }} r
left join (select distinct _id, manufacturingDays from {{ source('mongo', 'b2b_core_rfq_response_daily_snapshot') }} ) c
    on r.order_rfq_response_id = c._id
left join expensive as e on r.order_rfq_response_id = e.id
where rfq_sent_ts_msk >= cast(current_date() - interval 90 days as string) AND merchant_id is not null
group by merchant_id
),

products as (
    select 
    count(distinct product_id) as products,
    count(distinct category_id) as categories,
    merchant_id
    from
    (select distinct 
    p.product_id,
    category_id,
    merchant_id
    from {{ ref('sat_published_product') }} p
    inner join {{ ref('sat_product_state') }} s on p.product_id = s.product_id
    where s.next_effective_ts_msk is null and p.next_effective_ts_msk is null
    and s.status = 1
    )
    group by merchant_id
),

rfq_metrics as (select 
    coalesce(rfq_stat.merchant_id, products.merchant_id) as merchant_id,
    coalesce(valid_merchant, FALSE) as valid_merchant,
    rfq_answers,
    rfqs,
    time as rfq_time,
    time_corrected as rfq_time_corrected,
    responsed as rfq_validated,
    responsed_with_prices as rfq_validated_with_price,
    rfq_incorrect,
    rfq_expensive,
    rfq_orders_converted,
    rfq_answer_converted,
    rfq_categories,
    rfq_products,
    rfq_manufacturing_days,
    rfq_with_attached_docks,
    products,
    categories
    from rfq_stat full join products on products.merchant_id = rfq_stat.merchant_id
    ),
    
psi as (
select product_id, merchant_order_id, psi_ready, psi_start, psi_end_time, psis
from {{ ref('fact_psi') }}
),

orders as (
select *, 
 case when end_date is not null and start_date is not null and end_date >= start_date
    then datediff(end_date, start_date) end as manufacturing_days
from
(select *
,
case when advance_payment_acquired is null then date_add(manufacturing, 4) 
    else advance_payment_acquired end as start_date,
case when psi_ready_order is not null then date_add(psi_ready_order, -3) 
    else date_add(manufacturing_ended, -8) end as end_date
    from
(select 
p.merchant_order_id,
product_id,
order_id,
merchant_id,
manufacturing,
manufacturing_ended,
advance_payment_acquired,
manufacturing_and_qc_in_progress,
man_days,
product_id,
max(psi_ready) over (partition by p.merchant_order_id) as psi_ready_order, 
psi_ready,
psi_end_time, 
psis
from {{ ('production_stream_metrics') }} p 
left join psi on p.merchant_order_id = psi.merchant_order_id
where order_id is not null and merchant_id is not null
)
)
),

orders_stat as (
select 
    merchant_id,
    avg(declared_manufacturing_days) as declared_manufacturing_days,
    avg(manufacturing_days) as manufacturing_days,
    count(distinct merchant_order_id) as merchant_orders,
    count(distinct case when manufacturing_days is not null then merchant_order_id end) as produced_orders,
    count(distinct case when manufacturing_days > declared_manufacturing_days then merchant_order_id end) as orders_late,
    avg(case when manufacturing_days > declared_manufacturing_days 
        then manufacturing_days-declared_manufacturing_days end) as days_late
    from
    (
    select distinct 
    merchant_id,
    merchant_order_id,
    max(man_days) as declared_manufacturing_days,
    max(manufacturing_days) as manufacturing_days
    from orders
    where manufacturing >= cast(current_date() - interval 90 days as string)
    group by merchant_id, merchant_order_id
    )
    group by merchant_id
),

products_stat as (
select 
count(distinct product_id||merchant_order_id) as psis,
count(distinct case when psis > 1 then product_id||merchant_order_id end) as psis_failed,
avg(psis) as average_psi_attemps,
merchant_id
from orders
where psi_end_time is not null
group by merchant_id
),


production_metrics as (select 
    o.merchant_id,
    declared_manufacturing_days,
    manufacturing_days,
    merchant_orders,
    produced_orders,
    orders_late,
    days_late,
    psis,
    psis_failed,
    average_psi_attemps
from 
orders_stat o left join products_stat p on o.merchant_id = p.merchant_id)

select coalesce(rfq_metrics.merchant_id, production_metrics.merchant_id) as merchant_id,
    coalesce(valid_merchant, FALSE) as valid_merchant,
    rfq_answers,
    rfqs,
    rfq_time,
    rfq_time_corrected,
    rfq_validated,
    rfq_validated_with_price,
    rfq_incorrect,
    rfq_expensive,
    rfq_orders_converted,
    rfq_answer_converted,
    rfq_categories,
    rfq_products,
    rfq_manufacturing_days,
    rfq_with_attached_docks,
    products,
    categories,
    declared_manufacturing_days,
    manufacturing_days,
    merchant_orders,
    produced_orders,
    orders_late,
    days_late,
    psis,
    psis_failed,
    average_psi_attemps,
    date('{{ var("start_date_ymd") }}') as partition_date_msk
    from rfq_metrics full join production_metrics 
    on production_metrics.merchant_id = rfq_metrics.merchant_id
    where valid_merchant is True and 
    {% if is_incremental() %}
      and DATE(event_ts_msk) >= date('{{ var("start_date_ymd") }}')
      and DATE(event_ts_msk) < date('{{ var("end_date_ymd") }}')
    {% else %}
      and DATE(event_ts_msk)  >= date('2023-01-18')
    {% endif %}
