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
from {{ source( 'b2b_mart', 'operational_events') }}
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
select p.order_id, type, tag, stage, fee*(rate) as fee_rub
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

order_costs as (
select 
  order_id,
  dap_final_price_rub,
  exw_final_price_rub,
  dap_final_price_rub - exw_final_price_rub as logistics_final_price_rub,
  commission_final_price_rub,
  dap_final_price_rub/usd_rate as dap_final_price_usd,
  exw_final_price_rub/usd_rate as exw_final_price_usd,
  (dap_final_price_rub - exw_final_price_rub)/usd_rate as logistics_final_price_usd,
  commission_final_price_rub/usd_rate as commission_final_price_usd,
  dap_final_price_rub/cny_rate as dap_final_price_cny,
  exw_final_price_rub/cny_rate as exw_final_price_cny,
  (dap_final_price_rub - exw_final_price_rub)/cny_rate as logistics_final_price_cny,
  commission_final_price_rub/cny_rate as commission_final_price_cny,
  usd_rate_with_markup,
  usd_rate,
  cny_rate_with_markup,
  cny_rate
from prices 
),


client_payment as 
(
    select _id as order_id, case when payment.paymentType = 1 then payment.advancePercent/10000 when payment.paymentType = 2 then 100
    else 0 end as advance_percent,
    payment.paymentWithinDaysAdvance as days_advance,
    payment.paymentWithinDaysComplete as days_complete,
    payment.clientCurrency as client_currency,
    case when payment.paymentChannel = 1 then 'Internet projects' else 'CIA' end as payment_channel
    from {{ source('mongo', 'b2b_core_orders_v2_daily_snapshot') }}
),

order_hist as (
    select 
        order_id,
        date(event_ts_msk) as date_client_payed
    from {{ ref('fact_order_statuses_change') }}
    where status = 'manufacturing' and sub_status = 'joomSIAPaymentReceived'
),

orders as (
    select distinct order_id, friendly_id 
    from {{ ref('fact_order') }}
    where next_effective_ts_msk is null
),

m_statuses as (
  select payload.id as id,
    payload.status,
    min(TIMESTAMP(millis_to_ts_msk(payload.updatedTime))) as day
    from {{ source('b2b_mart', 'operational_events') }}
    WHERE type  ='merchantOrderChanged'
    group by payload.id,
        payload.status
    
),

merchant_order_schedule as
(
select 
    _id as merchant_order_id, 
    orderId as order_id,
    merchantId as merchant_id,
    friendlyId as merchant_order_friendly_id,
    min(date(case when payment_status = "noOperationsStarted" then day end)) as no_operations_started,
    min(date(case when payment_status = "advancePaymentRequested" then day end)) as advance_payment_requested,
    min(date(case when payment_status = "advancePaymentInProgress" then day end)) as advance_payment_in_progress,
    min(date(case when payment_status = "advancePaymentAcquired" then day end)) as advance_payment_acquired,
    min(date(case when payment_status = "manufacturingAndQcInProgress" then day end)) as manufacturing_and_qc_in_progress,
    min(date(case when payment_status = "remainingPaymentRequested" then day end)) as remaining_payment_requested,
    min(date(case when payment_status = "remainingPaymentInProgress" then day end)) as remaining_payment_in_progress,
    min(date(case when payment_status = "remainingPaymentAcquired" then day end)) as remaining_payment_acquired,
    min(date(case when payment_status = "completePaymentRequested" then day end)) as complete_payment_requested,
    min(date(case when payment_status = "completePaymentInProgress" then day end)) as complete_payment_in_progress,
    min(date(case when payment_status = "completePaymentAcquired" then day end)) as complete_payment_acquired,
    min(date(case when payment_status = "merchantAcquiredPayment" then day end)) as merchant_acquired_payment
from 
(
        select *, first_value(payment_status) over (partition by _id, orderId, merchantId order by day desc) as last_payment_status
        from
    (
    select distinct _id, friendlyId, orderId, merchantId, manDays, daysAfterQC, day, s.status as payment_status
from {{ ref('scd2_merchant_orders_v2_snapshot') }} o
left join m_statuses s on o._id = s.id

)
    )
group by _id, orderId, merchantId, friendlyId
),

merchant_order_rate as (
select 
orderId as order_id, 
_id as merchant_order_id, 
merchantId as merchant_id,
max(advancePercent/1000000) as advance_percent, 
min(time) as time,
max(ccy) as currency,
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
from {{ ref('scd2_merchant_orders_v2_snapshot') }}  m
    
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

select 
    oc.*,
    o.friendly_id as order_friendly_id,
    cp.advance_percent as client_advance_percent,
    cp.days_advance as client_days_advance,
    client_currency,
    days_complete,
    cp.payment_channel as client_payment_channel,
    oh.date_client_payed as date_client_payed,
    mos.merchant_order_id,
    mos.merchant_id,
    mos.merchant_order_friendly_id,
    advance_payment_requested,
    advance_payment_in_progress,
    advance_payment_acquired,
    remaining_payment_requested,
    remaining_payment_in_progress,
    remaining_payment_acquired,
    mor.advance_percent as merchant_advance_percent, 
    mor.currency as merchant_currency,
    advance_payment*usd_rate as advance_payment_rub,
    remaining_payment*usd_rate as remaining_payment_rub,
    complete_payment*usd_rate as complete_payment_rub,
    advance_payment as advance_payment_usd,
    remaining_payment as remaining_payment_usd,
    complete_payment as complete_payment_usd,
    advance_payment*usd_rate/cny_rate as advance_payment_cny,
    remaining_payment*usd_rate/cny_rate as remaining_payment_cny,
    complete_payment*usd_rate/cny_rate as complete_payment_cny
from order_costs oc
left join client_payment cp on oc.order_id = cp.order_id
left join order_hist oh on oc.order_id = oh.order_id
left join orders o on oc.order_id = o.order_id
left join merchant_order_schedule mos on mos.order_id = oc.order_id
left join merchant_order_rate mor on mos.merchant_order_id = mor.merchant_order_id
