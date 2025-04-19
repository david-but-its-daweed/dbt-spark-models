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
        sum(if(type = 'MEDIA_REVENUE', price, 0)) as media_revenue,
        sum(if(type = 'SERVICE_FEE', price, 0)) as service_fee,
        sum(if(type = 'COMMISSION', price, 0)) - sum(if(type = 'COMMISSION_VAT', price, 0)) as commission_revenue
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and date_trunc('day', transaction_date) < current_date() 
    group by
        date_trunc('day', transaction_date),
        date_trunc('month', transaction_date),
        date_trunc('quarter', transaction_date)
),


ads_spends_data as 
(
    select
        date_trunc('day', campaign_date_utc) as ads_spends_date,
        date_trunc('month', campaign_date_utc) as ads_spends_month,
        date_trunc('quarter', campaign_date_utc) as ads_spends_quarter,
        sum(spend) as spend,
        sum(clicks) as clicks
    from {{source('onfy_mart', 'ads_spends')}}
    where 1=1
        and campaign_date_utc < current_date()
    group by 
        date_trunc('day', campaign_date_utc),
        date_trunc('month', campaign_date_utc),
        date_trunc('quarter', campaign_date_utc)
),

base as 
(
    select 
        current_date() as date_updated,
        'day' as mode,

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
        order_data.commission_revenue,
    
        ads_spends_data.spend,
        ads_spends_data.clicks,

        ads_spends_data.spend / order_data.orders as general_cpo,
        ads_spends_data.spend / order_data.first_orders as general_cppu
    from order_data
    full join ads_spends_data
        on coalesce(order_data.transaction_date, '') = coalesce(ads_spends_data.ads_spends_date, '')
        and coalesce(order_data.transaction_month, '') = coalesce(ads_spends_data.ads_spends_month, '')
        and order_data.transaction_quarter = ads_spends_data.ads_spends_quarter
),

precalculation as 
(
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
),

forecasting as 
(
    select 
        date_updated,
        'forecasted day' as mode,
        current_month,
        max_date,
        dts.dates,
        precalculation.month,
        precalculation.quarter,
        avg(gmv_initial) as gmv_avg_distributed,
        avg(gross_profit_final) as gross_avg_distributed,
        avg(promocode_discount) as promocode_discount_avg_distributed,
        avg(orders) as orders_avg_distributed,
        avg(first_orders) as first_orders_avg_distributed,
        avg(first_gmv_initial) as first_gmv_initial_avg_distributed,
        avg(first_gross_profit_final) as first_gross_profit_final_avg_distributed,
        avg(first_promocode_discount) as first_promocode_discount_avg_distributed,
        avg(gmv_initial / orders) as average_check_avg_distributed,
        avg(first_gmv_initial / first_orders) as first_average_check_avg_distributed,
        avg(media_revenue) as media_revenue_avg_distributed,
        avg(service_fee) as service_fee_avg_distributed,
        avg(commission_revenue) as commission_revenue_avg_distributed,
    
        avg(spend) as spend_avg_distributed,
        avg(clicks) as clicks_avg_distributed,
    
        avg(spend / orders) as general_cpo_avg_distributed,
        avg(spend / first_orders) as general_cppu_avg_distributed,
        
        last_n_days,
        forecast_base
    from precalculation
    join 
        (
            select
                explode(sequence(current_date(), date_add(current_date(), date_diff(last_day(current_date()), current_date())))) AS dates,
                date_trunc("month", current_date()) as month
        ) as dts
        on dts.month = precalculation.month
    where 1=1
        and current_month
        and last_n_days
    group by 
        date_updated,
        current_month,
        max_date,
        dates,
        precalculation.month,
        precalculation.quarter,
        last_n_days,
        forecast_base
)

select 
    *
from precalculation
union all
select 
    *
from forecasting
