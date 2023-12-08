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


with sessions as 
(
    select distinct
        lower(device_events.payload.params.utm_source) as utm_source,
        lower(device_events.payload.params.utm_campaign) as utm_campaign,
        lower(coalesce(device_events.payload.params.utm_medium, device_events.payload.traffic_medium, payload.fb_install_referrer_campaign_name)) as utm_medium,
        coalesce(if(split(device_events.payload.link, '/')[2] like '%?utm%', left(split(device_events.payload.link, '/')[2], 8), split(device_events.payload.link, '/')[2]), 
        device_events.payload.link) as pzn,
        device_events.payload.link,
        partition_date_cet,
        device_id,
        event_ts_cet as session_ts_cet,
        event_id as event_id,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_session_ts_cet,
        count(device_id) over (partition by lower(payload.params.utm_source), lower(payload.params.utm_campaign), lower(payload.params.utm_medium), partition_date_cet) as campaign_sessions
    from 
        {{ source('onfy_mart', 'device_events') }} as device_events
     where 1=1
        and device_events.partition_date_cet >= current_date() - interval 91 days
        and device_events.type = 'externalLink'
        and lower(device_events.payload.params.utm_source) like '%facebook%'
        and device_events.payload.link like '%artikel%'
),

transactions as 
(
    select 
        order_id,
        device_id,
        transaction_date,
        sum(if(type = 'DISCOUNT', price, 0)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial
    from {{ source('onfy', 'transactions') }}
    where 1=1
        and currency = 'EUR'
        and transaction_date >= current_date() - interval 91 days
    group by 
        order_id,
        transaction_date,
        device_id
)


select
    pzn,
    count(distinct transactions.order_id) as payments,
    sum(if(sessions.partition_date_cet >= current_date() - interval 31 days, spend / campaign_sessions, 0)) as spend,
    count(distinct if(sessions.partition_date_cet >= current_date() - interval 31 days, sessions.event_id, null)) as sessions,
    sum(gmv_initial) as gmv_initial,
    sum(cast(promocode_discount as float)) as promocode_discount,
    sum(gross_profit_initial) as gross_profit_initial
from sessions
left join transactions
    on sessions.device_id = transactions.device_id
    and transactions.transaction_date between sessions.session_ts_cet and coalesce(next_session_ts_cet, current_timestamp())
    and to_unix_timestamp(transactions.transaction_date) - to_unix_timestamp(sessions.session_ts_cet) <= 86400
left join {{ source('onfy_mart', 'ads_spends') }} as ads_spends
    on sessions.partition_date_cet = ads_spends.campaign_date_utc
    and sessions.utm_source = lower(ads_spends.source)
    and sessions.utm_medium = lower(ads_spends.medium)
    and sessions.utm_campaign = lower(split_part(ads_spends.campaign_name, ' ', 1))
group by 
    pzn
having 
    count(distinct transactions.order_id) = 0
    or 
        (
            sum(if(sessions.partition_date_cet >= current_date() - interval 31 days, spend / campaign_sessions, 0)) 
            / count(distinct if(transaction_date >= current_date() - interval 31 days, transactions.order_id, null)) >= 40
            
            and
            
            count(distinct if(sessions.partition_date_cet >= current_date() - interval 31 days, sessions.event_id, null)) >= 50
            
            and 
            
            count(distinct if(transaction_date >= current_date() - interval 31 days, transactions.order_id, null)) > 0
            
        )
