{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


with merchant_orders as 
(select
merchant_order_id,
merchant_id,
fo.merchant_type,
created_ts_msk
from {{ ref('fact_merchant_order')}} fo
left join (
    select distinct _id, case when typ = 1 or typ is null then 'External' else 'Interanal' end as merchant_type
    from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}) mt on fo.merchant_order_id = mt._id
where mt.merchant_type is not null)
, 
internal_merchants as (
select _id as merchant_id, from_unixtime(activationTimeMs/1000 + 10800) as activation_time, 'internal' as merchant_type,
companyName as company_name, to_date(effective_ts) as min_ts,
explode(sequence(to_date(effective_ts), LEAST(nvl( to_date(next_effective_ts) - 1, current_date()), current_date()), interval 1 day)) as day
from {{ source('b2b_mart', 'dim_merchant') }}
where enabled = true and __is_deleted = false and __is_current = true
),

external_merchants as (
select merchant_id, 
created_ts_msk as activation_time, 'external' as merchant_type, company_name, to_date(created_ts_msk) as min_ts,
explode(sequence(to_date(created_ts_msk), current_date(), interval 1 day)) as day
from {{ ref('fact_merchant_order')}} fo
left join (
    select distinct merchantId as _id, merchant.compLegName as company_name,
    row_number() over (partition by merchantId order by case when merchant.compLegName is not null then 1 else 2 end, ctms desc)
    from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
    ) mt on fo.merchant_order_id = mt._id
where merchant_id not in (select distinct _id from b2b_mart.dim_merchant)
),

all_merchants as (
select 
    nvl(company_merchant_id, merchant_id) as merchant_id,
    activation_time, 
    merchant_type, 
    all.company_name,
    day,
    case when 
        sum(case when merchant_type = 'internal' then 1 else 0 end) over (partition by merchant_id) > 0 and 
        sum(case when merchant_type = 'external' then 1 else 0 end) over (partition by merchant_id) > 0
    then 'was external'
    else merchant_type end as merchant_types
from (
    select * from internal_merchants 
    union all 
    select * from external_merchants 
    where merchant_id || '' || day not in 
    (
        select distinct merchant_id || '' || day from internal_merchants
    )
) all 
left join (
    select max(case when merchant_type = 'internal' then merchant_id end) as company_merchant_id,
    company_name
    from (
    select distinct merchant_type, merchant_id, company_name from internal_merchants 
    union all 
    select distinct merchant_type, merchant_id, company_name from external_merchants 
    )
    where company_name is not null
    group by company_name
) comp on all.company_name = comp.company_name
where day >= current_date() - 120
)

select 
    am.merchant_id,
    am.activation_time, 
    case when am.activation_time is not null and am.activation_time <= day and am.merchant_type = 'internal' then 'internal activated' 
     when am.merchant_type = 'internal' then 'internal not activated' 
     else am.merchant_types end as activated_account,
    am.merchant_type, 
    am.company_name,
    am.day,
    am.merchant_types,
    max(case when mo.merchant_order_id is not null then 1 else 0 end) as active
from all_merchants am
left join merchant_orders mo on am.merchant_id = mo.merchant_id and mo.created_ts_msk >= day -30 and mo.created_ts_msk <= day
group by  am.merchant_id,
    am.activation_time, 
    case when am.activation_time is not null and am.activation_time > day and am.merchant_type = 'internal' then 'internal activated' 
     when am.merchant_type = 'internal' then 'internal not activated' 
     else am.merchant_types end,
    am.merchant_type, 
    am.company_name,
    am.day,
    am.merchant_types
