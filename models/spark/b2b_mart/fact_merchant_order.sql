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

WITH merchants AS (
    SELECT merchant_id, max(merchant_type) as merchant_type,
            min(created_ts_msk) as first_order_created
        FROM {{ ref('scd2_mongo_merchant_order') }}
    WHERE dbt_valid_to IS NULL
    GROUP BY merchant_id
),

orders as (
    select distinct order_id, last_order_status, last_order_sub_status, min_manufactured_ts_msk
    from {{ ref('fact_order') }}
    where next_effective_ts_msk is null
),


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
) as order_price_usd
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
 )

SELECT DISTINCT
       t.merchant_order_id ,
       TIMESTAMP(created_ts_msk) AS created_ts_msk,
       currency ,
       order_price_usd,
       firstmile_channel_id ,
       friendly_id,
       manufacturing_days,
       t.merchant_id,
       m.merchant_type,
       first_order_created,
       t.order_id,
       afterpayment_done,
       days_after_qc,
       prepay_percent,
       prepayment_done,
       product_id,
       product_type,
       product_vat_rate,
       payment_method_type,
       payment_method_id,
       last_order_status, last_order_sub_status, min_manufactured_ts_msk,
       TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
       TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_merchant_order') }} t
LEFT JOIN merchants m on t.merchant_id = m.merchant_id
LEFT JOIN merchant_gmv g ON t.merchant_id = g.merchant_id AND t.merchant_order_id = g.merchant_order_id
LEFT JOIN orders o ON t.order_id = o.order_id
WHERE deleted IS NULL
