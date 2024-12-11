{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}
    
with price_history as 
(
    select 
        effective_ts,
        product_id,
        pzn,
        manufacturer_name,
        min(price) as min_price
    from {{source('onfy_mart', 'dim_product')}}
    where 1=1
        and store_state = 'DEFAULT'
        and is_current
        and coalesce(stock_quantity, 0) > 0
        and effective_ts >= current_date() - interval 4 days
    group by 
        effective_ts,
        product_id,
        pzn,
        manufacturer_name
),

price_change as 
(
    select
        effective_ts,
        product_id,
        pzn,
        if(lag(min_price) over (partition by product_id order by effective_ts) > min_price, 1, 0) as price_change
    from price_history
),

latest_price as 
(
    select 
        max(effective_ts) as max_effective_ts,
        product_id,
        pzn,
        max_by(price_change, effective_ts) as latest_price_decrease
    from price_change
    group by 
        product_id,
        pzn
),

manufacturers_products as 
(
    select distinct 
        product_id
    from price_history
    where 1=1
        and manufacturer_name = 'MEDICE Arzneimittel Pütter GmbH&Co.KG'
),

billiger_sessions as 
(
    select distinct
        device_events.payload.params.utm_source as utm_source,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        if(event_ts_cet >= current_date() - interval 90 days, event_id, null) as window_event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet
    from 
        {{source('onfy_mart', 'device_events')}} as device_events
     where 1=1
        and device_events.partition_date_cet >= current_date() - interval 97 days
        and device_events.payload.params.utm_source like '%billiger%'
        and device_events.payload.link is not null
),

billiger_transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT' and transaction_date >= current_date() - interval 90 days, price, 0)) as promocode_discount,
        sum(if(transaction_date >= current_date() - interval 90 days, gmv_initial, 0)) as gmv_initial,
        sum(if(transaction_date >= current_date() - interval 90 days, gross_profit_initial, 0)) as gross_profit_initial
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and transaction_date >= current_date() - interval 91 days
    group by 
        order_id,
        transaction_date,
        device_id
),

billiger_data as 
(
    select
        utm_source,
        billiger_sessions.pzn,
        count(distinct billiger_sessions.event_id) as clicks,
        count(distinct billiger_transactions.order_id) as payments,
        if(
            count(distinct billiger_transactions.order_id) = 0 or 
            sum(gross_profit_initial + promocode_discount) / (count(distinct billiger_sessions.window_event_id) * 0.28 + sum(promocode_discount)) <= 0.7, 0, 
            count(distinct billiger_transactions.order_id) / count(distinct billiger_sessions.event_id)
        ) as cr,
        latest_price.latest_price_decrease,
        count(distinct billiger_sessions.event_id) * 0.28 as cost,
        sum(cast(promocode_discount as float)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial,
        sum(gross_profit_initial + promocode_discount) / (count(distinct billiger_sessions.window_event_id) * 0.28 + sum(promocode_discount)) as roas

    from 
        billiger_sessions
    left join billiger_transactions
        on billiger_sessions.device_id = billiger_transactions.device_id
        and billiger_transactions.transaction_date between billiger_sessions.session_ts_cet and coalesce(billiger_sessions.next_session_ts_cet, current_timestamp())
        and to_unix_timestamp(billiger_transactions.transaction_date) - to_unix_timestamp(billiger_sessions.session_ts_cet) <= 86400
    left join latest_price
        on latest_price.pzn = billiger_sessions.pzn
    group by 
        utm_source,
        billiger_sessions.pzn,
        latest_price_decrease
),

idealo_sessions as 
(
    select distinct
        device_events.payload.params.utm_source as utm_source,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        if(event_ts_cet >= current_date() - interval 90 days, event_id, null) as window_event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet
    from 
        {{source('onfy_mart', 'device_events')}} as device_events
     where 1=1
        and device_events.partition_date_cet >= current_date() - interval 97 days
        and device_events.payload.params.utm_source like '%idealo%'
        and device_events.payload.link is not null
),

idealo_transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT' and transaction_date >= current_date() - interval 90 days, price, 0)) as promocode_discount,
        sum(if(transaction_date >= current_date() - interval 90 days, gmv_initial, 0)) as gmv_initial,
        sum(if(transaction_date >= current_date() - interval 90 days, gross_profit_initial, 0)) as gross_profit_initial
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
        and transaction_date >= current_date() - interval 91 days
    group by 
        order_id,
        transaction_date,
        device_id
),

idealo_data as 
(
    select
        utm_source,
        idealo_sessions.pzn,
        count(distinct idealo_sessions.event_id) as clicks,
        count(distinct idealo_transactions.order_id) as payments,
        if(
            count(distinct idealo_transactions.order_id) = 0 or 
            sum(gross_profit_initial + promocode_discount) / (count(distinct idealo_sessions.window_event_id) * 0.44 + sum(promocode_discount)) <= 0.5, 0, 
            count(distinct idealo_transactions.order_id) / count(distinct idealo_sessions.event_id)
        ) as cr,
        latest_price.latest_price_decrease,
        count(distinct idealo_sessions.event_id) * 0.44 as cost,
        sum(cast(promocode_discount as float)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial,
        sum(gross_profit_initial + promocode_discount) / (count(distinct idealo_sessions.window_event_id) * 0.44 + sum(promocode_discount)) as roas

    from 
        idealo_sessions
    left join idealo_transactions
        on idealo_sessions.device_id = idealo_transactions.device_id
        and idealo_transactions.transaction_date between idealo_sessions.session_ts_cet and coalesce(next_session_ts_cet, current_timestamp())
        and to_unix_timestamp(idealo_transactions.transaction_date) - to_unix_timestamp(idealo_sessions.session_ts_cet) <= 86400
    left join latest_price
        on latest_price.pzn = idealo_sessions.pzn
    group by 
        utm_source,
        idealo_sessions.pzn,
        latest_price_decrease
),

google_data as 
(    
    select
        if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2]) as landing_pzn,
        count(distinct order_id) as orders,
        sum(session_spend) as spend,
        count(sources.device_id) as sessions
    from {{source('onfy', 'sources')}} as sources
    join {{source('onfy', 'ads_dashboard')}} as ads_dashboard
        on sources.device_id = ads_dashboard.device_id
        and sources.source_dt = ads_dashboard.session_dt
    where 1=1
        and session_dt >= current_date() - interval 3 month
        and source = 'google'
        and campaign like 'shopping%'
        and if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2]) is not null
        and len(if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2])) = 8
    group by 
        if(split(sources.landing_page, '/')[2] like '%?utm%', left(split(sources.landing_page, '/')[2], 8), split(sources.landing_page, '/')[2])
    having orders = 0
),

petersberg_exclusion as 
(
    select
        order_id,
        oi.pzn,
        oi.product_id,
        case
          when oi.store_name = 'Frauenapotheke.de' then before_products_price*0.1
          when oi.store_name = 'Fliegende-Pillen' then before_products_price*0.11
          else before_products_price*0.14 
          end as commission,
        oi.quantity,
        item_price,
        before_item_price,
        (before_products_price - products_price) as pa_spend,
        price as pb_price,
        price * oi.quantity as pb_before_products_price,
        dim_product.store_id,
        price * oi.quantity * 0.085 as pb_commission,
        if(price <= item_price, 0, price - item_price) as pb_pa_spend
    from {{source('onfy', 'orders_info')}} as oi
    join {{source('onfy_mart', 'dim_product')}} as dim_product
        on oi.pzn = dim_product.pzn
        and dim_product.is_current
        and dim_product.store_name = 'Petersberg Apotheke'
    where 1=1 
        and date(order_created_time_cet) >= current_date() - interval 31 days
        and oi.store_name <> 'Petersberg Apotheke'
        and before_item_price <> item_price
),

precalculation as 
(
    select 
        m.id as product_id, 
        null as store_id, 
        'google' as channel, 
        31 as weight, 
        'filter_out' as pessimization_type, 
        0.0 as discount_percent, 
        'blacklist' as source, 
        'Google Shopping low performing blacklist' as comment
    from google_data cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.landing_pzn
    where orders < 1
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'ohm' as channel, 
        31 as weight, 
        'filter_out' as pessimization_type, 
        0.0 as discount_percent, 
        'blacklist' as source, 
        'OHM Google Shopping low performing blacklist' as comment
    from google_data cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.landing_pzn
    where orders < 1
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'idealo' as channel, 
        31 as weight, 
        'use_max_price' as pessimization_type, 
        0.0 as discount_percent, 
        'low_performing' as source, 
        'Idealo low performing blacklist' as comment
    from idealo_data as cr
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.pzn
    where (cr.cr <= 0.001 and cr.latest_price_decrease <> 1)
    union all
    select 
        m.id as product_id, 
        null as store_id, 
        'billiger' as channel, 
        31 as weight, 
        'use_max_price' as pessimization_type, 
        0.0 as discount_percent, 
        'low_performing' as source, 
        'Billiger low performing blacklist' as comment
    from billiger_data cr 
    join {{source('pharmacy_landing', 'medicine')}} m 
        on m.country_local_id = cr.pzn
    where (cr.cr <= 0.001 and cr.latest_price_decrease <> 1)
    union all 
    select 
        product_id,
        store_id,
        'idealo' as channel,
        31 as weight,
        null as pessimization_type,
        0.0 as discount_percent,
        'low_commission' as source,
        'Petersberg low commission items' as comment
    from petersberg_exclusion
    where 1=1
        and pb_price <= before_item_price
    group by 
        product_id,
        store_id
    having 
        sum(commission - pa_spend) > sum(pb_commission - pb_pa_spend)
    union all 
    select 
        product_id,
        store_id,
        'billiger' as channel,
        31 as weight,
        null as pessimization_type,
        0.0 as discount_percent,
        'low_commission' as source,
        'Petersberg low commission items' as comment
    from petersberg_exclusion
    where 1=1
        and pb_price <= before_item_price
    group by 
        product_id,
        store_id
    having 
        sum(commission - pa_spend) > sum(pb_commission - pb_pa_spend)
)

select *
from precalculation 
where 1=1
    and product_id not in (select distinct product_id from manufacturers_products)


