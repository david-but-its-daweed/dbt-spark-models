{{ config(
    schema='b2b_mart',
    partition_by=['partition_date_msk'],
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false',
      'priority_weight': '150'
    }
) }}


WITH currencies_list_v1 as 
(
select distinct explode(split(key, '-')) as currency
from 
(
select explode(rates)
from
(select payload.currencies.*,
row_number() over (partition by payload.orderId order by payload.updatedTime desc) as rn 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
    and payload.updatedTime is not null 
    {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
    and partition_date   >= date'2022-05-19'
     {% endif %}
)
where rn = 1 
)
),

currencies_list as (
    select
        t1.currency as from,
        t2.currency as to,
        t1.currency||'-'||t2.currency as currency
    from currencies_list_v1 as t1 cross join currencies_list_v1 as t2
),


currencies as 
(
select event_id, currencies.rates as rates, currencies.companyRates as company_rates
from
(select event_id, payload.currencies from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
    and payload.updatedTime is not null
    {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
    and partition_date   >= date'2022-05-19'
     {% endif %}
)
),

order_rates as (
select * from
(
select event_id, 
case when from = to then 1 else rates[currency]['exchangeRate'] end as rate, 
case when from = to then 0 else rates[currency]['markupRate'] end as markup_rate, 
from, to
from currencies t1 
cross join currencies_list
order by event_id
)
where rate is not null
),

typed_prices as 
(select
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    type,
    tag, 
    stage,
    col.amount as fee,
    col.ccy as currency
from
(
select 
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    type,
    tag,
    posexplode_outer(col.multiPrice),
    col.stage as stage
from
(
select
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    col.type as type,
    col.tag as tag,
    posexplode_outer(col.stagedPrices)

from
(select event_id,
        partition_date AS partition_date_msk,
        coalesce(TIMESTAMP(millis_to_ts_msk(payload.updatedTime)), event_ts_msk) as event_ts_msk,
        payload.orderId AS order_id,
        payload.clientCurrency AS client_currency,
        posexplode_outer(payload.typedPricesOriginal),
        payload.reason AS reason,
        payload.roleSet.roles.owner.actualisationTime.time as owner_time,
        payload.roleSet.roles.owner.actualisationTime.type as owner_type,
        payload.roleSet.roles.owner.moderatorId as owner_moderator_id,
        payload.roleSet.roles.owner.roleType as owner_role_type,
        payload.roleSet.roles.bizDev.actualisationTime.time as biz_dev_time,
        payload.roleSet.roles.bizDev.actualisationTime.type as biz_dev_type,
        payload.roleSet.roles.bizDev.moderatorId as biz_dev_moderator_id,
        payload.roleSet.roles.bizDev.roleType as biz_dev_role_type,
        payload.status AS status,
        payload.subStatus AS sub_status,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
    and payload.updatedTime is not null
    {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
    and partition_date   >= date'2022-05-19'
     {% endif %}
)
)
)
),

other_prices as 
(select 
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    type,
    tag, 
    stage,
    col.amount as fee,
    col.ccy as currency
from
(
select 
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    type,
    tag,
    posexplode_outer(col.multiPrice),
    col.stage as stage
from
(
select
    event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    col.type as type,
    col.tag as tag,
    posexplode_outer(col.stagedPrices)

from
(select event_id,
        partition_date AS partition_date_msk,
        coalesce(TIMESTAMP(millis_to_ts_msk(payload.updatedTime)), event_ts_msk) as event_ts_msk,
        payload.orderId AS order_id,
        payload.clientCurrency AS client_currency,
        posexplode_outer(payload.otherPricesOriginal),
        payload.reason AS reason,
        payload.roleSet.roles.owner.actualisationTime.time as owner_time,
        payload.roleSet.roles.owner.actualisationTime.type as owner_type,
        payload.roleSet.roles.owner.moderatorId as owner_moderator_id,
        payload.roleSet.roles.owner.roleType as owner_role_type,
        payload.roleSet.roles.bizDev.actualisationTime.time as biz_dev_time,
        payload.roleSet.roles.bizDev.actualisationTime.type as biz_dev_type,
        payload.roleSet.roles.bizDev.moderatorId as biz_dev_moderator_id,
        payload.roleSet.roles.bizDev.roleType as biz_dev_role_type,
        payload.status AS status,
        payload.subStatus AS sub_status,
        payload.gmv.clientConvertedGMV AS client_converted_gmv,
        payload.gmv.finalGMV AS final_gmv,
        payload.gmv.finalGrossProfit AS final_gross_profit,
        payload.gmv.initialGrossProfit AS initial_gross_profit
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
    and payload.updatedTime is not null
    {% if is_incremental() %}
       and partition_date >= date'{{ var("start_date_ymd") }}'
       and partition_date < date'{{ var("end_date_ymd") }}'
     {% else %}
    and partition_date   >= date'2022-05-19'
     {% endif %}
)
)
)
),

a as (
select
prices.event_id,
    partition_date_msk,
    event_ts_msk,
    order_id,
    client_currency,
    reason,
    owner_time,
    owner_type,
    owner_moderator_id,
    owner_role_type,
    biz_dev_time,
    biz_dev_type,
    biz_dev_moderator_id,
    biz_dev_role_type,
    status,
    sub_status,
    client_converted_gmv,
    final_gmv,
    final_gross_profit,
    initial_gross_profit,
    type,
    tag, 
    stage,
    fee *
        (case when from.to = 'USD' then from.rate*(1 - to.markup_rate)
            when to.from = 'USD' then 1/to.rate/(1 - to.markup_rate)
        end) as fee,
    fee as fee_original,
    from.rate as from_rate,
    to.rate as to_rate,
    currency
from 
(
select * from typed_prices
union all
select * from other_prices
) prices
left join order_rates as from on from.event_id = prices.event_id and currency = from.from and from.to = 'USD'
left join order_rates as to on to.event_id = prices.event_id and currency = to.to and to.from = 'USD'
)

SELECT a.event_id,
        partition_date_msk AS partition_date_msk,
        TIMESTAMP(a.event_ts_msk) AS event_ts_msk,
        a.order_id,
        client_currency,
        reason ,
        TIMESTAMP(coalesce(millis_to_ts_msk(owner_time), a.event_ts_msk)) AS owner_time_msk,
        owner_type,
        owner_moderator_id,
        owner_role_type,
        TIMESTAMP(coalesce(millis_to_ts_msk(biz_dev_time), a.event_ts_msk)) AS biz_dev_time_msk,
        biz_dev_type,
        biz_dev_moderator_id,
        biz_dev_role_type,
        a.status,
        a.sub_status,
        SUM(case when tag = 'grant' and stage = 'final' then -fee when stage = 'final' then fee end) AS total_final_price,
        SUM(IF(tag = 'ddp' and stage = 'final', fee, 0)) AS ddp_final_price,
        SUM(IF(tag = 'dap' and stage = 'final', fee, 0)) AS dap_final_price,
        SUM(IF(tag = 'ewx' and stage = 'final', fee, 0)) AS ewx_final_price,
        SUM(IF(tag = 'exw' and stage = 'final', fee, 0)) AS exw_final_price,
        SUM(IF(type = 'firstmileBeforeQC' and stage = 'final', fee, 0)) AS firstmile_before_qc_final_price,
        SUM(IF(type = 'warehousingOp' and stage = 'final', fee, 0)) AS warehousing_op_final_price,
        SUM(IF(type = 'firstmileAfterQC' and stage = 'final', fee, 0)) AS firstmile_after_qc_final_price,
        SUM(IF(type = 'utilityFee' and stage = 'final', fee, 0)) AS utility_fee_final_price,
        SUM(IF(type = 'customsFee' and stage = 'final', fee, 0)) AS customs_fee_final_price,
        SUM(IF(type = 'loadingUnloading' and stage = 'final', fee, 0)) AS loading_unloading_final_price,
        SUM(IF(type = 'labeling' and stage = 'final', fee, 0)) AS labeling_final_price,
        SUM(IF(type = 'commission' and stage = 'final', fee, 0)) AS commission_final_price,
        SUM(IF(type = 'merchantFee' and stage = 'final', fee, 0)) AS merchant_fee_final_price,
        SUM(IF(type = 'warehousing' and stage = 'final', fee, 0)) AS warehousing_final_price,
        SUM(IF(type = 'customsDuty' and stage = 'final', fee, 0)) AS customs_duty_final_price,
        SUM(IF(type = 'brokerFee' and stage = 'final', fee, 0)) AS broker_fee_final_price,
        SUM(IF(type = 'linehaul' and stage = 'final', fee,  0)) AS linehaul_final_price,
        SUM(IF(type = 'lastMile' and stage = 'final', fee, 0)) AS last_mile_final_price,
        SUM(IF(type = 'qc' and stage = 'final', fee,  0)) AS qc_final_price,
        SUM(IF(type = 'agentFee' and stage = 'final', fee,  0)) AS agent_fee_final_price,
        SUM(IF(type = 'certification' and stage = 'final', fee,  0)) AS certification_final_price,
        SUM(IF(type = 'vat' and stage = 'final', fee,  0)) AS vat_final_price,
        SUM(IF(type = 'generalCargo' and stage = 'final', fee,  0)) AS general_cargo_final_price,
        SUM(case when tag = 'grant' and stage = 'confirmed' then -fee when stage = 'confirmed' then fee end) AS total_confirmed_price,
        SUM(IF(tag = 'ddp' and stage = 'confirmed', fee, 0)) AS ddp_confirmed_price,
        SUM(IF(tag = 'dap' and stage = 'confirmed', fee, 0)) AS dap_confirmed_price,
        SUM(IF(tag = 'ewx' and stage = 'confirmed', fee, 0)) AS ewx_confirmed_price,
        SUM(IF(tag = 'exw' and stage = 'confirmed', fee, 0)) AS exw_confirmed_price,
        SUM(IF(type = 'firstmileBeforeQC' and stage = 'confirmed', fee, 0)) AS firstmile_before_qc_confirmed_price,
        SUM(IF(type = 'warehousingOp' and stage = 'confirmed', fee, 0)) AS warehousing_op_confirmed_price,
        SUM(IF(type = 'firstmileAfterQC' and stage = 'confirmed', fee, 0)) AS firstmile_after_qc_confirmed_price,
        SUM(IF(type = 'utilityFee' and stage = 'confirmed', fee, 0)) AS utility_fee_confirmed_price,
        SUM(IF(type = 'customsFee' and stage = 'confirmed', fee, 0)) AS customs_fee_confirmed_price,
        SUM(IF(type = 'loadingUnloading' and stage = 'confirmed', fee, 0)) AS loading_unloading_confirmed_price,
        SUM(IF(type = 'labeling' and stage = 'confirmed', fee, 0)) AS labeling_confirmed_price,
        SUM(IF(type = 'commission' and stage = 'confirmed', fee, 0)) AS commission_confirmed_price,
        SUM(IF(type = 'merchantFee' and stage = 'confirmed', fee, 0)) AS merchant_fee_confirmed_price,
        SUM(IF(type = 'warehousing' and stage = 'confirmed', fee, 0)) AS warehousing_confirmed_price,
        SUM(IF(type = 'customsDuty' and stage = 'confirmed', fee, 0)) AS customs_duty_confirmed_price,
        SUM(IF(type = 'brokerFee' and stage = 'confirmed', fee, 0)) AS broker_fee_confirmed_price,
        SUM(IF(type = 'linehaul' and stage = 'confirmed', fee,  0)) AS linehaul_confirmed_price,
        SUM(IF(type = 'lastMile' and stage = 'confirmed', fee, 0)) AS last_mile_confirmed_price,
        SUM(IF(type = 'qc' and stage = 'confirmed', fee,  0)) AS qc_confirmed_price,
        SUM(IF(type = 'agentFee' and stage = 'confirmed', fee,  0)) AS agent_fee_confirmed_price,
        SUM(IF(type = 'certification' and stage = 'confirmed', fee,  0)) AS certification_confirmed_price,
        SUM(IF(type = 'vat' and stage = 'confirmed', fee,  0)) AS vat_confirmed_price,
        SUM(IF(type = 'generalCargo' and stage = 'confirmed', fee,  0)) AS general_cargo_confirmed_price,
        MAX(client_converted_gmv) AS client_converted_gmv,
        MAX(final_gmv) AS final_gmv,
        MAX(final_gross_profit) AS final_gross_profit,
        MAX(initial_gross_profit) AS initial_gross_profit,
        to_timestamp(a.event_ts_msk) AS ctms
FROM  a 
GROUP BY a.event_id,
        partition_date_msk,
        a.event_ts_msk,
        a.order_id,
        client_currency,
        reason,
        owner_time,
        owner_type,
        owner_moderator_id,
        owner_role_type,
        biz_dev_time,
        biz_dev_type,
        biz_dev_moderator_id,
        biz_dev_role_type,
        a.status,
        a.sub_status
