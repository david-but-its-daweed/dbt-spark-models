{{ config(
    schema='pharmacy',
    materialized='table',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'false',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

-- считаем за нулевую конверсию отсутствие покупки за все время,
-- и добавляем к этому оконный роас 
with sessions as 
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
        {{ source('onfy_mart', 'device_events') }} as device_events
     where 1=1
        and device_events.partition_date_cet >= '2023-05-01'
        and device_events.payload.params.utm_source like '%idealo%'
        and device_events.payload.link is not null
),

transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT' and transaction_date >= current_date() - interval 90 days, price, 0)) as promocode_discount,
        sum(if(transaction_date >= current_date() - interval 90 days, gmv_initial, 0)) as gmv_initial,
        sum(if(transaction_date >= current_date() - interval 90 days, gross_profit_initial, 0)) as gross_profit_initial
    from {{ source('onfy', 'transactions') }} 
    where 1=1
        and currency = 'EUR'
        and transaction_date >= '2023-05-01'
    group by 
        order_id,
        transaction_date,
        device_id
)

select
    utm_source,
    pzn,
    count(distinct sessions.event_id) as clicks,
    count(distinct transactions.order_id) as payments,
    if(
        count(distinct transactions.order_id) = 0 or 
        sum(gross_profit_initial + promocode_discount) / (count(distinct sessions.window_event_id) * 0.44) <= 0.35, 0, 
        count(distinct transactions.order_id) / count(distinct sessions.event_id)
    ) as cr,
    count(distinct sessions.event_id) * 0.44 as cost,
    sum(gmv_initial) as products_price_sum,
    sum(cast(promocode_discount as float)) as promocode_discount,
    sum(gross_profit_initial) + sum(promocode_discount) as gross_profit_final
from 
    sessions
left join transactions
    on sessions.device_id = transactions.device_id
    and transactions.transaction_date between sessions.session_ts_cet and coalesce(next_session_ts_cet, current_timestamp())
    and to_unix_timestamp(transactions.transaction_date) - to_unix_timestamp(sessions.session_ts_cet) <= 86400
group by 
    utm_source,
    pzn
