{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2023-01-27']
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

dim as (
    select distinct _id as merchant_id, 
    __is_current as is_current, 
    __is_deleted as is_deleted,
    name, companyName, enabled,
    DATE(millis_to_ts_msk(createdTimeMs)) as created_at
    from {{ source('b2b_mart', 'dim_merchant') }}
    where date(next_effective_ts) >= date('3030-01-01')
),

rfq_recieved_1 as (
    select merchantId, count(distinct rfq_request_id) as rfq_recieved, 1 as for_join
    from
    (select distinct merchantId, split(path, '/')[0] as categoryId 
     from
    {{ source('mongo', 'b2b_core_published_products_daily_snapshot') }} a
     join {{ source('mart', 'category_levels') }} b  on a.categoryId = b.category_id
     ) c
    left join (SELECT
        rfq_request_id,
        category_name as category_id
    FROM {{ ref('fact_rfq_requests') }}
    WHERE next_effective_ts_msk IS NULL 
     {% if is_incremental() %}
      and DATE(sent_ts_msk) >= date('{{ var("start_date_ymd") }}') - interval 90 days
    {% else %}
      and DATE(sent_ts_msk)  >= date('2023-02-08') - interval 90 days
    {% endif %}
     ) r on c.categoryId = r.category_id
    group by merchantId
),

rfq_recieved_2 as (
    select count(distinct rfq_request_id) as rfq_recieved, 1 as for_join
    from
    (select distinct merchantId, split(path, '/')[0] as categoryId 
     from
    {{ source('mongo', 'b2b_core_published_products_daily_snapshot') }} a
     join {{ source('mart', 'category_levels') }} b  on a.categoryId = b.category_id
     ) c
    left join (SELECT
        rfq_request_id,
        category_name as category_id
    FROM {{ ref('fact_rfq_requests') }}
    WHERE next_effective_ts_msk IS NULL 
     {% if is_incremental() %}
      and DATE(sent_ts_msk) >= date('{{ var("start_date_ymd") }}') - interval 90 days
    {% else %}
      and DATE(sent_ts_msk)  >= date('2023-02-08') - interval 90 days
    {% endif %}
     ) r on c.categoryId = r.category_id
),

rfq_recieved as (
    select merchantId, coalesce(r1.rfq_recieved, r2.rfq_recieved) as rfq_recieved
    from rfq_recieved_1 r1 left join rfq_recieved_2 r2 on r1.for_join = r2.for_join
),

rfq_stat as
(    
select 
  merchant_id,
  count(distinct order_rfq_response_id) as rfq_answers,
  count(distinct rfq_request_id) as rfqs,
  AVG(time_rfq_response) as time,
  percentile_approx(CASE WHEN
      rfq_merchant_rn = 1
      AND time_rfq_response >= percentile_10 AND time_rfq_response <= percentile_90
    THEN time_rfq_response
  END, 0.5) AS time_corrected,
  
  avg(percentile_10) as percentile_10,
  
  avg(percentile_90) as percentile_90,
    
  count(distinct case when response_status in ('accepted', 'rejected') then order_rfq_response_id end) as responsed,
  count(
      distinct case when response_status in ('accepted', 'rejected') and different_prices then order_rfq_response_id end
      ) as responsed_with_prices,
  count(distinct case when reject_reason 
      in ('totalyDifferent', 'littleDifferent', 'termsNotSuit', 'incorrectPackaging') 
      then order_rfq_response_id end) as rfq_incorrect,
  count(distinct case when reject_reason 
      in ('expensive') and different_prices and not expensive
      then order_rfq_response_id end) as rfq_expensive,
  count(distinct case when converted = 1 then order_id end) as rfq_orders_converted,
  count(distinct case when converted = 1 then order_rfq_response_id end) as rfq_answer_converted,
  count(distinct rfq_request_id) >= 4 as valid_merchant,
  count(distinct case when (reject_reason is null or reject_reason not
      in ('totalyDifferent', 'littleDifferent', 'termsNotSuit', 'incorrectPackaging') )
      and category_id > '' then category_id end) as rfq_categories,
  count(distinct product_id) as rfq_products,
  count(distinct case when documents_attached then order_rfq_response_id end) as rfq_with_attached_docks,
  avg(manufacturingDays) as rfq_manufacturing_days,
  max(rfq_recieved) as rfq_recieved
from (select *, 
  percentile_approx(CASE WHEN rfq_merchant_rn = 1 then time_rfq_response END, 0.1) over (partition by merchant_id) as percentile_10,
  percentile_approx(CASE WHEN rfq_merchant_rn = 1 then time_rfq_response END, 0.9) over (partition by merchant_id) as percentile_90
  from {{ ref('rfq_metrics') }}) r
left join (select distinct _id, manufacturingDays from {{ source('mongo', 'b2b_core_rfq_response_daily_snapshot') }} ) c
    on r.order_rfq_response_id = c._id
left join expensive as e on r.order_rfq_response_id = e.id
left join rfq_recieved rr on r.merchant_id = rr.merchantId
where 
    1=1 
    {% if is_incremental() %}
      and DATE(rfq_sent_ts_msk) >= date('{{ var("start_date_ymd") }}') - interval 90 days
    {% else %}
      and DATE(rfq_sent_ts_msk)  >= date('2023-02-08') - interval 90 days
    {% endif %}
AND merchant_id is not null
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
    rfq_recieved,
    rfq_answers,
    rfqs,
    time as rfq_time,
    time_corrected as rfq_time_corrected,
    percentile_10 as rfq_time_percentile_10,
    percentile_90 as rfq_time_percentile_90,
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
from {{ ref('production_stream_metrics') }} p 
left join psi on p.merchant_order_id = psi.merchant_order_id
where order_id is not null and merchant_id is not null
    {% if is_incremental() %}
      and DATE(manufacturing) >= date('{{ var("start_date_ymd") }}') - interval 90 days
    {% else %}
      and DATE(manufacturing)  >= date('2023-02-08') - interval 90 days
    {% endif %}
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
orders_stat o left join products_stat p on o.merchant_id = p.merchant_id),

currencies_list_v1 as 
(
select 'USD' as from, 1 as for_join
union all
select 'RUB' as from, 1 as for_join
union all
select 'EUR' as from, 1 as for_join
union all
select 'CNY' as from, 1 as for_join
),

currencies_list as (
    select t1.from, t2.from as to, t1.from||'-'||t2.from as currency, 1 as for_join
    from currencies_list_v1 as t1 left join currencies_list_v1 as t2 on t1.for_join = t2.for_join
),


currencies as 
(
select order_id, currencies.rates as rates, currencies.companyRates as company_rates, 1 as for_join
from
(
select orderId as order_id, currencies, row_number() over (
    partition by orderId order by currencies.rates is not null desc, currencies.companyRates is not null desc, updatedTime) as rn
from
(select payload.* from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
and payload.updatedTime is not null and payload.status = 'manufacturing'
order by updatedTime desc
)
)
where rn = 1
),

order_rates as(
select * from
(
select order_id, 
case when from = to then 1 else rates[currency]['exchangeRate'] end as rate, 
case when from = to then 0 else rates[currency]['markupRate'] end as markup_rate, 
case when from = to then 1 else company_rates[currency]['exchangeRate'] end as company_rate,
from, to
from currencies t1 
left join currencies_list t2 on t1.for_join = t2.for_join
order by order_id
)
where rate is not null
),

merchant_gmv as (
select merchant_id, merchant_order_id,
sum(
    case when complete_payment>0 then complete_payment
    when remaining_payment>0 then remaining_payment + advance_payment
    else advance_payment/advance_percent end
) as gmv
from
(
select 
orderId as order_id, 
_id as merchant_order_id, 
merchantId as merchant_id,
max(advancePercent/1000000) as advance_percent, 
min(time) as time,
max(case when status = 20 then coalesce(rate, 1) *amount/1000000 else 0 end) as advance_payment,
max(case when status = 40 then coalesce(rate, 1) *amount/1000000 else 0 end) as remaining_payment,
max(case when status = 60 then coalesce(rate, 1) *amount/1000000 else 0 end) as complete_payment
from
(
select 
orderId, _id, 
advancePercent, 
merchantId,
paymentType, history.paymentStatus as status, from_unixtime(history.utms/1000) as time, 
history.requestedPrice.amount, history.requestedPrice.ccy, 
coalesce(order_rates.rate, 1/order_rates_1.rate) as rate
from
(
select payment.advancePercent, payment.paymentType, 
explode(payment.paymentStatusHistory) as history, orderId, _id, merchantId,
paymentSchedule 
from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}  m
    inner join (
    select distinct 
    merchant_id,
    merchant_order_id
    from orders
) o on m.merchantId = o.merchant_id and m._id = o.merchant_order_id
    
) price
left join order_rates on order_rates.from = price.history.requestedPrice.ccy and order_rates.to = 'USD' 
    and order_rates.order_id = price.orderId
left join order_rates order_rates_1 on order_rates_1.to = price.history.requestedPrice.ccy and order_rates_1.from = 'USD'
    and order_rates_1.order_id = price.orderId
where history.paymentStatus in (20, 40, 60)
)
group by orderId, _id, 
merchantId
)
where 1=1
group by merchant_id, merchant_order_id
 ),
 
orders1 AS (
    SELECT
        order_id, user_id,
        DATE(min_manufactured_ts_msk) AS partition_date_msk
    FROM {{ ref('fact_order') }}
    WHERE next_effective_ts_msk IS NULL
),

merchant_orders1 as (
select distinct order_id, merchant_id, merchant_order_id
from {{ ref('fact_merchant_order') }}
),

retention AS (
    SELECT
        merchant_id,
        COUNT(DISTINCT CASE WHEN prev_period_order = 1 THEN user_id END) AS prev_order,
        COUNT(DISTINCT CASE WHEN current_period_order = 1 THEN user_id END) AS current_order,
        COUNT(DISTINCT CASE WHEN prev_period_order = 1 AND current_period_order = 1 THEN user_id END) AS prev_current_order,
        SUM(CASE WHEN prev_period_order = 1 THEN prev_final_gmv ELSE 0 END) AS prev_final_gmv,
        SUM(CASE WHEN prev_period_order = 1 AND current_period_order = 1 THEN prev_final_gmv ELSE 0 END) AS prev_current_final_gmv,
        SUM(CASE WHEN current_period_order = 1 THEN current_final_gmv ELSE 0 END) AS current_final_gmv,
        SUM(CASE WHEN prev_period_order = 1 AND current_period_order = 1 THEN current_final_gmv ELSE 0 END) AS current_prev_final_gmv
    FROM
        (SELECT
            o.user_id,
            i.merchant_id,
            MAX(CASE WHEN o.partition_date_msk > date('{{ var("start_date_ymd") }}') - interval 180 days
                AND o.partition_date_msk <= date('{{ var("start_date_ymd") }}') - interval 90 days
                THEN 1 ELSE 0 END) AS prev_period_order,
            MAX(CASE WHEN o.partition_date_msk > date('{{ var("start_date_ymd") }}') - interval 90 days
                THEN 1 ELSE 0 END) AS current_period_order,
            SUM(CASE WHEN o.partition_date_msk > date('{{ var("start_date_ymd") }}') - interval 180 days
                AND o.partition_date_msk <= date('{{ var("start_date_ymd") }}') - interval 90 days
                THEN gmv ELSE 0 END) AS prev_final_gmv,
            SUM(CASE WHEN o.partition_date_msk > date('{{ var("start_date_ymd") }}') - interval 90 days
                THEN gmv ELSE 0 END) AS current_final_gmv
            from orders1 AS o 
            left join merchant_orders1 i ON i.order_id = o.order_id
         left join merchant_gmv ON i.merchant_order_id = merchant_gmv.merchant_order_id
            GROUP BY
                o.user_id,
                i.merchant_id
        )
    GROUP BY merchant_id
)

select distinct
    coalesce(rfq_metrics.merchant_id, production_metrics.merchant_id, dim.merchant_id) as merchant_id,
    coalesce(valid_merchant, FALSE) as valid_merchant,
    rfq_recieved,
    rfq_answers,
    rfqs,
    rfq_time,
    rfq_time_corrected,
    rfq_time_percentile_10,
    rfq_time_percentile_90,
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
    gmv,
    prev_order,
    current_order,
    prev_current_order,
    prev_final_gmv,
    prev_current_final_gmv,
    current_final_gmv,
    current_prev_final_gmv,
    is_current, 
    is_deleted,
    name, 
    companyName as company_name,
    enabled,
    created_at,
    date('{{ var("start_date_ymd") }}') as partition_date_msk
    from rfq_metrics 
    full join production_metrics on production_metrics.merchant_id = rfq_metrics.merchant_id
    full join dim on coalesce(rfq_metrics.merchant_id, production_metrics.merchant_id) = dim.merchant_id
    left join (select merchant_id, sum(gmv) as gmv from merchant_gmv group by merchant_id) gmv 
        on coalesce(rfq_metrics.merchant_id, production_metrics.merchant_id, dim.merchant_id) = gmv.merchant_id
    left join retention on coalesce(rfq_metrics.merchant_id, production_metrics.merchant_id, dim.merchant_id) = retention.merchant_id
