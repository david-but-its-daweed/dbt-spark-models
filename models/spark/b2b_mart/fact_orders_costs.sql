{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
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
case when from = to then 1 else company_rates[currency]['exchangeRate'] end as company_rate,
from, to
from currencies t1 
left join currencies_list t2 on t1.for_join = t2.for_join
order by order_id
)
where rate is not null
),

typed_prices as 
(select order_id, type, tag, stage, col.amount as fee, col.ccy as currency
from
(
select order_id, type, tag, explode(col.multiPrice), col.stage as stage
from
(
select order_id, col.type as type, col.tag as tag, explode(col.stagedPrices)
from
(
select orderId as order_id, explode(typedPricesOriginal), rn
from
(select payload.*, row_number() over (partition by payload.orderId order by payload.updatedTime desc) as rn 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
and payload.updatedTime is not null and payload.status = 'manufacturing'
order by updatedTime desc
)
)
where rn = 1 and col is not null
)
)
),

other_prices as 
(select order_id, type, tag, stage, col.amount as fee, col.ccy as currency
from
(
select order_id, type, tag, explode(col.multiPrice), col.stage as stage
from
(
select order_id, col.type as type, col.tag as tag, explode(col.stagedPrices)
from
(
select orderId as order_id, explode(otherPricesOriginal), rn
from
(select payload.*, row_number() over (partition by payload.orderId order by payload.updatedTime desc) as rn 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
and payload.updatedTime is not null and payload.status = 'manufacturing'
order by updatedTime desc
)
)
where rn = 1 and col is not null
)
)
),

all_prices as 
(select order_id, type, tag, stage, sum(fee_rub) as fee_rub
from
(
select p.order_id, type, tag, stage, fee*(rate*(1+markup_rate)) as fee_rub
from
(
select distinct * from typed_prices 
union all 
select distinct * from other_prices
) p
left join order_rates r on p.order_id = r.order_id and p.currency = r.from and r.to = 'RUB'
)
group by order_id, type, tag, stage
),

all_orders AS (
  SELECT DISTINCT u.user_id, u.order_id, u.min_manufactured_ts_msk as date, friendly_id
  FROM {{ ref('fact_order') }} u
  LEFT JOIN {{ ref('fact_user_request') }} f ON f.user_id = u.user_id
  WHERE (is_joompro_employee != TRUE or is_joompro_employee IS NULL) and u.next_effective_ts_msk is null
  and u.min_manufactured_ts_msk is not null and u.friendly_id != 'KXMZQ'
),

admins as (
    select distinct admin_id,
    email
    from {{ ref('dim_user_admin') }}
),

order_owners as (
    select distinct order_id, owner_id, email as owner_email 
    from
    (select distinct 
        order_id,
        first_value(owner_moderator_id) over (partition by order_id order by event_ts_msk) as owner_id
        from {{ ref('fact_order_change') }}
        where status = 'manufacturing'
    ) o
    left join admins a 
    on o.owner_id = a.admin_id 
),

prices_final as (
select p.order_id, 
        round(SUM(fee_rub), 2) AS ddp_final_price_rub,
        round(SUM(IF(tag in ('dap', 'exw', 'ewx'), fee_rub, 0)), 2) AS dap_final_price_rub,
        round(SUM(IF(tag = 'ewx', fee_rub, 0)), 2) AS ewx_final_price_rub,
        round(SUM(IF(tag = 'exw', fee_rub, 0)), 2) AS exw_final_price_rub,
        round(SUM(IF(type = 'firstmileBeforeQC', fee_rub, 0)), 2) AS firstmile_before_qc_final_price_rub,
        round(SUM(IF(type = 'warehousingOp', fee_rub, 0)), 2) AS warehousing_op_final_price_rub,
        round(SUM(IF(type = 'firstmileAfterQC', fee_rub, 0)), 2) AS firstmile_after_qc_final_price_rub,
        round(SUM(IF(type = 'utilityFee', fee_rub, 0)), 2) AS utility_fee_final_price_rub,
        round(SUM(IF(type = 'customsFee', fee_rub, 0)), 2) AS customs_fee_final_price_rub,
        round(SUM(IF(type = 'loadingUnloading', fee_rub, 0)), 2) AS loading_unloading_final_price_rub,
        round(SUM(IF(type = 'labeling', fee_rub, 0)), 2) AS labeling_final_price_rub,
        round(SUM(IF(type = 'commission', fee_rub, 0)), 2) AS commission_final_price_rub,
        round(SUM(IF(type = 'merchantFee', fee_rub, 0)), 2) AS merchant_fee_final_price_rub,
        round(SUM(IF(type = 'warehousing', fee_rub, 0)), 2) AS warehousing_final_price_rub,
        round(SUM(IF(type = 'customsDuty', fee_rub, 0)), 2) AS customs_duty_final_price_rub,
        round(SUM(IF(type = 'brokerFee', fee_rub, 0)), 2) AS broker_fee_final_price_rub,
        round(SUM(IF(type = 'linehaul', fee_rub, 0)), 2) AS linehaul_final_price_rub,
        round(SUM(IF(type = 'lastMile', fee_rub, 0)), 2) AS last_mile_final_price_rub,
        round(SUM(IF(type = 'qc', fee_rub,  0)), 2) AS qc_final_price_rub,
        round(SUM(IF(type = 'agentFee', fee_rub, 0)), 2) AS agent_fee_final_price_rub,
        round(SUM(IF(type = 'certification', fee_rub, 0)), 2) AS certification_final_price_rub,
        round(SUM(IF(type = 'vat', fee_rub, 0)), 2) AS vat_final_price_rub,
        round(SUM(IF(type = 'generalCargo', fee_rub, 0)), 2) AS general_cargo_final_price_rub,
        max(usd_rate) as usd_rate,
        max(usd_company_rate) as usd_company_rate,
        max(usd_markup_rate) as usd_markup_rate,
        max(usd_rate_with_markup) as usd_rate_with_markup,
        max(cny_rate) as cny_rate,
        max(cny_company_rate) as cny_company_rate,
        max(cny_markup_rate) as cny_markup_rate,
        max(cny_rate_with_markup) as cny_rate_with_markup
        from all_prices p
        left join 
        (
        select order_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by order_id
        ) r on p.order_id = r.order_id
        where fee_rub is not null and stage = 'final'
        group by p.order_id
),

prices_forecast as (
select p.order_id, 
        round(SUM(fee_rub), 2) AS ddp_forecast_price_rub,
        round(SUM(IF(tag in ('dap', 'exw', 'ewx'), fee_rub, 0)), 2) AS dap_forecast_price_rub,
        round(SUM(IF(tag = 'ewx', fee_rub, 0)), 2) AS ewx_forecast_price_rub,
        round(SUM(IF(tag = 'exw', fee_rub, 0)), 2) AS exw_forecast_price_rub,
        round(SUM(IF(type = 'firstmileBeforeQC', fee_rub, 0)), 2) AS firstmile_before_qc_forecast_price_rub,
        round(SUM(IF(type = 'warehousingOp', fee_rub, 0)), 2) AS warehousing_op_forecast_price_rub,
        round(SUM(IF(type = 'firstmileAfterQC', fee_rub, 0)), 2) AS firstmile_after_qc_forecast_price_rub,
        round(SUM(IF(type = 'utilityFee', fee_rub, 0)), 2) AS utility_fee_forecast_price_rub,
        round(SUM(IF(type = 'customsFee', fee_rub, 0)), 2) AS customs_fee_forecast_price_rub,
        round(SUM(IF(type = 'loadingUnloading', fee_rub, 0)), 2) AS loading_unloading_forecast_price_rub,
        round(SUM(IF(type = 'labeling', fee_rub, 0)), 2) AS labeling_forecast_price_rub,
        round(SUM(IF(type = 'commission', fee_rub, 0)), 2) AS commission_forecast_price_rub,
        round(SUM(IF(type = 'merchantFee', fee_rub, 0)), 2) AS merchant_fee_forecast_price_rub,
        round(SUM(IF(type = 'warehousing', fee_rub, 0)), 2) AS warehousing_forecast_price_rub,
        round(SUM(IF(type = 'customsDuty', fee_rub, 0)), 2) AS customs_duty_forecast_price_rub,
        round(SUM(IF(type = 'brokerFee', fee_rub, 0)), 2) AS broker_fee_forecast_price_rub,
        round(SUM(IF(type = 'linehaul', fee_rub, 0)), 2) AS linehaul_forecast_price_rub,
        round(SUM(IF(type = 'lastMile', fee_rub, 0)), 2) AS last_mile_forecast_price_rub,
        round(SUM(IF(type = 'qc', fee_rub,  0)), 2) AS qc_forecast_price_rub,
        round(SUM(IF(type = 'agentFee', fee_rub, 0)), 2) AS agent_fee_forecast_price_rub,
        round(SUM(IF(type = 'certification', fee_rub, 0)), 2) AS certification_forecast_price_rub,
        round(SUM(IF(type = 'vat', fee_rub, 0)), 2) AS vat_forecast_price_rub,
        round(SUM(IF(type = 'generalCargo', fee_rub, 0)), 2) AS general_cargo_forecast_price_rub,
        max(usd_rate) as usd_rate,
        max(usd_company_rate) as usd_company_rate,
        max(usd_markup_rate) as usd_markup_rate,
        max(usd_rate_with_markup) as usd_rate_with_markup,
        max(cny_rate) as cny_rate,
        max(cny_company_rate) as cny_company_rate,
        max(cny_markup_rate) as cny_markup_rate,
        max(cny_rate_with_markup) as cny_rate_with_markup
        from all_prices p
        left join 
        (
        select order_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by order_id
        ) r on p.order_id = r.order_id
        where fee_rub is not null and stage = 'confirmed'
        group by p.order_id
),

subsidy as (
select p.order_id, 
        round(SUM(IF(type in  ('grant', 'dapGrant') or tag in ('grant', 'dapGrant'), fee_rub, 0)), 2) AS subsidy_confirmed_price_rub
        from all_prices p
        left join 
        (
        select order_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by order_id
        ) r on p.order_id = r.order_id
        where fee_rub is not null and stage in ('confirmed', 'grant')
        group by p.order_id
),

prices as (
    select distinct
        pf.*, ddp_forecast_price_rub,
        dap_forecast_price_rub,
        ewx_forecast_price_rub,
        exw_forecast_price_rub,
        firstmile_before_qc_forecast_price_rub,
        warehousing_op_forecast_price_rub,
        firstmile_after_qc_forecast_price_rub,
        utility_fee_forecast_price_rub,
        customs_fee_forecast_price_rub,
        loading_unloading_forecast_price_rub,
        labeling_forecast_price_rub,
        commission_forecast_price_rub,
        merchant_fee_forecast_price_rub,
        warehousing_forecast_price_rub,
        customs_duty_forecast_price_rub,
        broker_fee_forecast_price_rub,
        linehaul_forecast_price_rub,
        last_mile_forecast_price_rub,
        qc_forecast_price_rub,
        agent_fee_forecast_price_rub,
        certification_forecast_price_rub,
        vat_forecast_price_rub,
        general_cargo_forecast_price_rub,
        subsidy_confirmed_price_rub
    from prices_final pf
    left join prices_forecast p on pf.order_id = p.order_id
    left join subsidy g on pf.order_id = g.order_id

),

gmv as 
(
    SELECT DISTINCT 
        order_id,
        gmv_initial,
        initial_gross_profit,
        final_gross_profit
    FROM {{ ref('gmv_by_sources') }}
)

select distinct
        o.order_id, 
        o.friendly_id,
        o.user_id,
        ddp_final_price_rub,
        dap_final_price_rub,
        ewx_final_price_rub,
        exw_final_price_rub,
        firstmile_before_qc_final_price_rub,
        warehousing_op_final_price_rub,
        firstmile_after_qc_final_price_rub,
        utility_fee_final_price_rub,
        customs_fee_final_price_rub,
        loading_unloading_final_price_rub,
        labeling_final_price_rub,
        commission_final_price_rub,
        merchant_fee_final_price_rub,
        warehousing_final_price_rub,
        customs_duty_final_price_rub,
        broker_fee_final_price_rub,
        linehaul_final_price_rub,
        last_mile_final_price_rub,
        qc_final_price_rub,
        agent_fee_final_price_rub,
        certification_final_price_rub,
        vat_final_price_rub,
        general_cargo_final_price_rub,
        ddp_forecast_price_rub,
        dap_forecast_price_rub,
        ewx_forecast_price_rub,
        exw_forecast_price_rub,
        firstmile_before_qc_forecast_price_rub,
        warehousing_op_forecast_price_rub,
        firstmile_after_qc_forecast_price_rub,
        utility_fee_forecast_price_rub,
        customs_fee_forecast_price_rub,
        loading_unloading_forecast_price_rub,
        labeling_forecast_price_rub,
        commission_forecast_price_rub,
        merchant_fee_forecast_price_rub,
        warehousing_forecast_price_rub,
        customs_duty_forecast_price_rub,
        broker_fee_forecast_price_rub,
        linehaul_forecast_price_rub,
        last_mile_forecast_price_rub,
        qc_forecast_price_rub,
        agent_fee_forecast_price_rub,
        certification_forecast_price_rub,
        vat_forecast_price_rub,
        general_cargo_forecast_price_rub,
        subsidy_confirmed_price_rub,
        usd_rate,
        usd_company_rate,
        usd_markup_rate,
        usd_rate_with_markup,
        cny_rate,
        cny_company_rate,
        cny_markup_rate,
        cny_rate_with_markup,
        owner_email,
        date,
        gmv_initial,
        initial_gross_profit,
        final_gross_profit,
        c.company_name, c.grade, c.grade_probability
from prices p
join all_orders o on o.order_id = p.order_id
join order_owners u on u.order_id = o.order_id
left join (
     select distinct company_name, user_id, grade, grade_probability from {{ ref('users_daily_table') }}
     where partition_date_msk = (select max(partition_date_msk) from {{ ref('users_daily_table') }})
     ) c on o.user_id = c.user_id
left join gmv g on g.order_id = o.order_id 
