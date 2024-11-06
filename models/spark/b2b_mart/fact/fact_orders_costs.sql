{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

WITH currencies_list_v1 AS (
    SELECT DISTINCT EXPLODE(SPLIT(key, '-')) AS currency
    FROM (
        SELECT EXPLODE(rates)
        FROM (
            SELECT
                payload.currencies.*,
                ROW_NUMBER() OVER (PARTITION BY payload.orderId ORDER BY payload.updatedTime DESC) AS rn 
            FROM {{ source('b2b_mart', 'operational_events') }}
            WHERE
                type = 'orderChangedByAdmin'
                AND payload.updatedTime IS NOT NULL
                {% if is_incremental() %}
                AND partition_date >= date'{{ var("start_date_ymd") }}'
                AND partition_date < date'{{ var("end_date_ymd") }}'
                {% else %}
                and partition_date   >= date'2022-05-19'
                {% endif %}
        )
        WHERE rn = 1
    )
),

currencies_list AS (
    SELECT
        t1.currency AS from,
        t2.currency AS to,
        t1.currency||'-'||t2.currency as currency
    FROM currencies_list_v1 AS t1
    CROSS JOIN currencies_list_v1 AS t2
),


currencies AS (
    SELECT
        event_id,
        order_id,
        currencies.rates AS rates,
        currencies.companyRates AS company_rates
    FROM (
        SELECT
            event_id,
            payload.orderId as order_id,
            payload.currencies
        FROM {{ source('b2b_mart', 'operational_events') }}
        WHERE
            type = 'orderChangedByAdmin'
            AND payload.updatedTime IS NOT NULL
            {% if is_incremental() %}
            AND partition_date >= date'{{ var("start_date_ymd") }}'
            AND partition_date < date'{{ var("end_date_ymd") }}'
            {% else %}
            AND partition_date   >= date'2022-05-19'
            {% endif %}
    )
),

order_rates as (
select * from
(
select 
order_id,
event_id,
case when from = to then 1 else rates[currency]['exchangeRate'] end as rate, 
case when from = to then 0 else rates[currency]['markupRate'] end as markup_rate, 
case when from = to then 1 else company_rates[currency]['exchangeRate'] end as company_rate,
from, to
from currencies t1 
cross join currencies_list
order by event_id
)
where rate is not null
),
    
typed_prices as 
(select order_id, type, tag, stage, col.amount as fee, col.ccy as currency, event_id
from
(
select order_id, type, tag, explode(col.multiPrice), col.stage as stage, event_id
from
(
select order_id, col.type as type, col.tag as tag, explode(col.stagedPrices), event_id
from
(
select event_id, orderId as order_id, explode(typedPricesOriginal), rn
from
(select event_id, payload.*, row_number() over (partition by payload.orderId order by payload.updatedTime desc) as rn 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
and payload.updatedTime is not null and payload.status in ('manufacturing', 'shipping', 'claim', 'done')
order by updatedTime desc
)
)
where rn = 1 and col is not null
)
)
),

country as (
    select distinct user_id, coalesce(country, "RU") as country
    from {{ ref('dim_user') }}
    where next_effective_ts_msk is null
),

all_orders AS (
  SELECT DISTINCT u.user_id, u.order_id, u.min_manufactured_ts_msk as date, friendly_id, c.country,
    coalesce(u.delivery_scheme, 'EXW') as delivery_scheme
  FROM {{ ref('fact_order') }} u
  LEFT JOIN country c USING(user_id)
  WHERE u.next_effective_ts_msk is null
  and u.min_manufactured_ts_msk is not null and u.friendly_id != 'KXMZQ'
),

other_prices as 
(select order_id, type, tag, stage, col.amount as fee, col.ccy as currency, event_id
from
(
select order_id, type, tag, explode(col.multiPrice), col.stage as stage, event_id
from
(
select order_id, col.type as type, col.tag as tag, explode(col.stagedPrices), event_id
from
(
select event_id, orderId as order_id, explode(otherPricesOriginal), rn
from
(select event_id, payload.*, row_number() over (partition by payload.orderId order by payload.updatedTime desc) as rn 
from {{ source('b2b_mart', 'operational_events') }}
where type = 'orderChangedByAdmin'
and payload.updatedTime is not null and payload.status in ('manufacturing', 'shipping', 'claim', 'done')
order by updatedTime desc
)
)
where rn = 1 and col is not null
)
)
),

all_prices as 
(select event_id, order_id, replace(type, 'DDP', '') AS type, 
    case when tag = 'dap' and type != 'qc' and delivery_scheme = 'EXW' then 'ddp' else tag end as tag,
    stage, sum(fee_rub) as fee_rub
from
(
select p.event_id, p.order_id, type, tag, stage, fee*(rate*(1+markup_rate)) as fee_rub, ao.delivery_scheme
from
(
select distinct * from typed_prices 
union all 
select distinct * from other_prices
) p
left join order_rates r on p.event_id = r.event_id and p.currency = r.from and r.to = 'RUB'
left join all_orders ao on ao.order_id = p.order_id
)
group by event_id, order_id, type, case when tag = 'dap' and type != 'qc' and delivery_scheme = 'EXW' then 'ddp' else tag end, stage
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
        first_value(owner_moderator_id) over (partition by order_id order by event_ts_msk desc) as owner_id
        from {{ ref('fact_order_change') }}
        where status in ('manufacturing', 'shipping', 'claim', 'done')
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
        round(SUM(IF(type = 'currencyControl', fee_rub, 0)), 2) AS currency_control_final_price_rub,
        round(SUM(IF(type = 'exportDeclaration', fee_rub, 0)), 2) AS export_declaration_final_price_rub,
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
        select event_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by event_id
        ) r on p.event_id = r.event_id
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
        round(SUM(IF(type = 'currencyControl', fee_rub, 0)), 2) AS currency_control_forecast_price_rub,
        round(SUM(IF(type = 'exportDeclaration', fee_rub, 0)), 2) AS export_declaration_forecast_price_rub,
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
        select event_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by event_id
        ) r on p.event_id = r.event_id
        where fee_rub is not null and stage = 'confirmed'
        group by p.order_id
),

subsidy as (
select p.order_id, 
        round(SUM(IF(type in  ('grant', 'dapGrant') or tag in ('grant', 'dapGrant'), fee_rub, 0)), 2) AS subsidy_confirmed_price_rub,
        round(SUM(IF(type = 'firstmileBeforeQC', fee_rub, 0)), 2) AS firstmile_before_qc_confirmed_price_rub,
        round(SUM(IF(type = 'firstmileAfterQC', fee_rub, 0)), 2) AS firstmile_after_qc_confirmed_price_rub,
        round(SUM(IF(type = 'linehaul', fee_rub, 0)), 2) AS linehaul_confirmed_price_rub
        from all_prices p
        left join 
        (
        select 
        event_id,
        order_id, 
        max(case when from = 'USD' and to = 'RUB' then rate end) as usd_rate,
        max(case when from = 'USD' and to = 'RUB' then company_rate end) as usd_company_rate,
        max(case when from = 'USD' and to = 'RUB' then markup_rate end) as usd_markup_rate,
        max(case when from = 'USD' and to = 'RUB' then rate*(1+markup_rate) end) as usd_rate_with_markup,
        max(case when from = 'CNY' and to = 'RUB' then rate end) as cny_rate,
        max(case when from = 'CNY' and to = 'RUB' then company_rate end) as cny_company_rate,
        max(case when from = 'CNY' and to = 'RUB' then markup_rate end) as cny_markup_rate,
        max(case when from = 'CNY' and to = 'RUB' then rate*(1+markup_rate) end) as cny_rate_with_markup
        from order_rates
        group by order_id, event_id
        ) r on p.event_id = r.event_id
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
        currency_control_forecast_price_rub,
        export_declaration_forecast_price_rub,
        subsidy_confirmed_price_rub,
        firstmile_before_qc_confirmed_price_rub,
        firstmile_after_qc_confirmed_price_rub,
        linehaul_confirmed_price_rub
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
    FROM {{ ref('gmv_by_sources_wo_filters') }}
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
        currency_control_final_price_rub,
        export_declaration_final_price_rub,
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
        currency_control_forecast_price_rub,
        export_declaration_forecast_price_rub,
        subsidy_confirmed_price_rub,
        firstmile_before_qc_confirmed_price_rub,
        firstmile_after_qc_confirmed_price_rub,
        linehaul_confirmed_price_rub,
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
        c.company_name, c.grade, c.grade_probability,
        o.country, o.delivery_scheme
from prices p
join all_orders o on o.order_id = p.order_id
join order_owners u on u.order_id = o.order_id
left join (
     select distinct company_name, user_id, grade, grade_probability from {{ ref('users_daily_table') }}
     where partition_date_msk = (select max(partition_date_msk) from {{ ref('users_daily_table') }})
     ) c on o.user_id = c.user_id
left join gmv g on g.order_id = o.order_id 
