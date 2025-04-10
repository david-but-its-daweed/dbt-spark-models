{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'priority_weight': '100'
    }
) }}

with order_data as 
(
    select
        date_trunc('day', transaction_date) as transaction_date,
        date_trunc('month', transaction_date) as transaction_month,
        date_trunc('quarter', transaction_date) as transaction_quarter,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_final) as gross_profit_final,
        sum(if(type = 'DISCOUNT', price, 0)) as promocode_discount,
        count(distinct order_id) as orders,
        count(distinct if(purchase_num = 1, order_id, null)) as first_orders,
        sum(if(purchase_num = 1, gmv_initial, 0)) as first_gmv_initial,
        sum(if(purchase_num = 1, gross_profit_final, 0)) as first_gross_profit_final,
        sum(if(purchase_num = 1 and type = 'DISCOUNT', price, 0)) as first_promocode_discount,
        sum(gmv_initial) / count(distinct order_id) as average_check,
        sum(if(purchase_num = 1, gmv_initial, 0)) / count(distinct if(purchase_num = 1, order_id, null)) as first_average_check,
        sum(if(type = 'MEDIA_REVENUE', price, 0) as media_revenue,
        sum(if(type = 'SERVICE_FEE', price, 0) as service_fee
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and date_trunc('day', transaction_date) < current_date() 
    group by
        grouping sets (
            (date_trunc('day', transaction_date), date_trunc('month', transaction_date), date_trunc('quarter', transaction_date)), 
            (date_trunc('month', transaction_date), date_trunc('quarter', transaction_date)),
            (date_trunc('quarter', transaction_date))
        )
),


ads_spends_data as 
(
    select
        date_trunc('day', campaign_date_utc) as ads_spends_date,
        date_trunc('month', campaign_date_utc) as ads_spends_month,
        date_trunc('quarter', campaign_date_utc) as ads_spends_quarter,
        sum(spend) as spend,
        sum(clicks) as clicks
    from  {{source('onfy_mart', 'ads_spends')}}
    group by 
        grouping sets (
            (campaign_date_utc, date_trunc('month', campaign_date_utc), date_trunc('quarter', campaign_date_utc)), 
            (date_trunc('month', campaign_date_utc), date_trunc('quarter', campaign_date_utc)),
            (date_trunc('quarter', campaign_date_utc))
        )
),

base as 
(
    select 
        current_date() as date_updated,
        case
            when coalesce(transaction_date, ads_spends_date) is null and 
                coalesce(transaction_month, ads_spends_month) is not null and
                coalesce(transaction_quarter, ads_spends_quarter) is not null 
            then 'month'
            when coalesce(transaction_date, ads_spends_date) is null and 
                coalesce(transaction_month, ads_spends_month) is null and
                coalesce(transaction_quarter, ads_spends_quarter) is not null 
            then 'quarter'
            else 'day'
        end as mode,

        date_trunc('month', current_date()) = transaction_month as current_month,
        if(date_trunc('month', current_date()) = transaction_month, date_part('day', current_date() - interval 1 day), null) as max_date,

        coalesce(transaction_date, ads_spends_date) as date,
        coalesce(transaction_month, ads_spends_month) as month,
        coalesce(transaction_quarter, ads_spends_quarter) as quarter,
        order_data.gmv_initial,
        order_data.gross_profit_final,
        order_data.promocode_discount,
        order_data.orders,
        order_data.first_orders,
        order_data.first_gmv_initial,
        order_data.first_gross_profit_final,
        order_data.first_promocode_discount,
        order_data.average_check,
        order_data.first_average_check,
        order_data.media_revenue,
        order_data.service_fee,
    
        ads_spends_data.spend,
        ads_spends_data.clicks,

        ads_spends_data.spend / order_data.orders as general_cpo,
        ads_spends_data.spend / order_data.first_orders as general_cppu
    from order_data
    full join ads_spends_data
        on coalesce(order_data.transaction_date, '') = coalesce(ads_spends_data.ads_spends_date, '')
        and coalesce(order_data.transaction_month, '') = coalesce(ads_spends_data.ads_spends_month, '')
        and order_data.transaction_quarter = ads_spends_data.ads_spends_quarter
)

select 
    *,
    date between current_date() - interval 7 day and current_date() - interval 1 day as last_n_days,
    '7 days' as forecast_base
from base
union all
select 
    *,
    date between current_date() - interval 3 day and current_date() - interval 1 day as last_n_days,
    '3 days' as forecast_base
from base
union all
select 
    *,
    current_month as last_n_days,
    'month' as forecast_base
from base
