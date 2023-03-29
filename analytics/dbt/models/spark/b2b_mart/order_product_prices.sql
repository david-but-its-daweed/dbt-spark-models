{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}


with currencies_list_v1 as 
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
case when from = to then 1 else coalesce(company_rates[currency]['exchangeRate'],
                                       rates[currency]['exchangeRate'])  end as company_rate,
from, to
from currencies t1 
left join currencies_list t2 on t1.for_join = t2.for_join
order by order_id
)
where rate is not null
),

orders as (
select product_id, merchant_order_id,
value.priceAmountPerItem as price, 
currency,
value.qty
from
(select id as product_id, 
    merchOrdId as merchant_order_id, 
    explode(variants),
    currency
from {{ source('mongo', 'b2b_core_order_products_daily_snapshot') }}
)
)


select product_id, merchant_order_id, order_id, 
sum(price*qty*company_rate)/1000000 as amount
from
(
select o.*, fm.order_id, 
coalesce(r1.rate, 1/r2.rate, case when currency = 'USD' then 1 end) as rate, 
coalesce(r1.company_rate, 1/r2.company_rate, case when currency = 'USD' then 1 end) as company_rate from orders o left join 
(select distinct merchant_order_id, order_id from {{ ref('fact_merchant_order') }}) fm 
on o.merchant_order_id = fm.merchant_order_id
left join 
order_rates r1 on fm.order_id = r1.order_id and r1.from = currency and r1.to = 'USD'
left join 
order_rates r2 on fm.order_id = r2.order_id and r2.to = currency and r2.from = 'USD'
)
group by product_id, merchant_order_id, order_id
