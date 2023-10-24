{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring',
      'priority_weight': '100'
    }
) }}

with order_data as 
(
    select
        date_trunc('day', transaction_date) as transaction_date,
        date_trunc('month', transaction_date) as transaction_month,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_final) as gross_profit_final,
        sum(if(type = 'DISCOUNT', price, 0)) as promocode_discount,
        count(distinct order_id) as orders,
        count(distinct if(purchase_num = 1, order_id, null)) as first_orders,
        sum(if(purchase_num = 1, gmv_initial, 0)) as first_gmv_initial,
        sum(if(purchase_num = 1, gross_profit_final, 0)) as first_gross_profit_final,
        sum(if(purchase_num = 1 and type = 'DISCOUNT', price, 0)) as first_promocode_discount,
        sum(gmv_initial) / count(distinct order_id) as average_check,
        sum(if(purchase_num = 1, gmv_initial, 0)) / count(distinct if(purchase_num = 1, order_id, null)) as first_average_check
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
    group by
        grouping sets ((date_trunc('day', transaction_date), date_trunc('month', transaction_date)), (date_trunc('month', transaction_date)))
),


ads_spends_data as 
(
    select
        date_trunc('day', campaign_date_utc) as ads_spends_date,
        date_trunc('month', campaign_date_utc) as ads_spends_month,
        sum(spend) as spend,
        sum(clicks) as clicks
    from {{source('onfy_mart', 'ads_spends')}}
    group by 
        grouping sets ((campaign_date_utc, date_trunc('month', campaign_date_utc)), (date_trunc('month', campaign_date_utc)))
)

select 
    case
        when coalesce(transaction_date, ads_spends_date) is null and coalesce(transaction_month, ads_spends_month) is not null then 'month'
        else 'day'
    end as mode,
    
    coalesce(transaction_date, ads_spends_date) between current_date() - interval 7 day and current_date() - interval 1 day as last_7_days,
    coalesce(transaction_date, ads_spends_date) between current_date() - interval 3 day and current_date() - interval 1 day as last_3_days,
    
    date_trunc('month', current_date()) = transaction_month as current_month,
    if(date_trunc('month', current_date()) = transaction_month, date_part('day', current_date() - interval 1 day), null) as max_date,
    
    coalesce(transaction_date, ads_spends_date) as date,
    coalesce(transaction_month, ads_spends_month) as month,
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
    ads_spends_data.spend,
    ads_spends_data.clicks,
    
    ads_spends_data.spend / order_data.orders as general_cpo,
    ads_spends_data.spend / order_data.first_orders as general_cppu
from order_data
full join ads_spends_data
    on coalesce(order_data.transaction_date, '') = coalesce(ads_spends_data.ads_spends_date, '')
    and order_data.transaction_month = ads_spends_data.ads_spends_month
