{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}


with uid as
(
    select
        device_id,
        app_device_type,
        min_by(user_email_hash, min_purchase_date) as user_email_hash
    from {{ source('onfy_mart', 'devices_mart')}}
    where 1=1
        and is_bot = False
    group by 
        device_id,
        app_device_type
),

sources as 
(
    select distinct
        device_id,
        type,
        event_ts_cet,
        coalesce(
            case 
                when onfy_mart.device_events.type = 'externalLink'
                then
                    case
                        when lower(onfy_mart.device_events.payload.params.utm_source) like '%google%' 
                        then 'google' 
                        when onfy_mart.device_events.payload.params.utm_source is not null
                        then onfy_mart.device_events.payload.params.utm_source
                        when 
                            (onfy_mart.device_events.payload.referrer like '%/www.google%') 
                            or (onfy_mart.device_events.payload.referrer like '%/www.bing%')
                            or (onfy_mart.device_events.payload.referrer like '%/search.yahoo.com%')
                            or (onfy_mart.device_events.payload.referrer like '%/duckduckgo.com%')
                        then 'Organic'
                        when
                            (onfy_mart.device_events.payload.referrer like '%facebook.com%') 
                            or (onfy_mart.device_events.payload.referrer like '%instagram.com%')             
                        then 'UNMARKED_facebook_or_instagram'      
                    end
                when onfy_mart.device_events.type in ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                then
                    case
                        when onfy_mart.device_events.payload.utm_source = 'Unattributed' then 'Facebook'
                        when onfy_mart.device_events.payload.utm_source is null then 'Unknown'
                        when onfy_mart.device_events.payload.utm_source = 'Google Organic Search' then 'Organic'
                        else onfy_mart.device_events.payload.utm_source
                    end
            end
        , 'Unknown') AS utm_source,
        case 
            when onfy_mart.device_events.type = 'externalLink' 
            then onfy_mart.device_events.payload.params.utm_campaign
            else onfy_mart.device_events.payload.utm_campaign 
        end as utm_campaign,
            case 
            when 
                lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%adchampaign%'
                or lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign))like '%adchampagn%' 
                then 'adchampagne'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%rocket%' then 'rocket10'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%whiteleads%' then 'whiteleads'
                when lower(coalesce(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) like '%ohm%' then 'ohm'
                else 'onfy'
            end as partner
    from {{ source('onfy_mart', 'device_events')}}
    where 1=1
        and type in ('externalLink', 'adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
),

corrected_sources as 
(
    select
        sources.*,
        lower(coalesce(utm.source_corrected, sources.utm_source)) as source_corrected,
        lower(coalesce(utm.campaign_corrected, sources.utm_campaign)) as campaign_corrected
    from sources
    left join pharmacy.utm_campaigns_corrected as utm
        on coalesce(lower(utm.utm_campaign), '') = coalesce(lower(sources.utm_campaign), '') 
        and coalesce(lower(utm.utm_source), '') = coalesce(lower(sources.utm_source), '') 
),

session_precalc as 
(
    select 
        type,
        device_id,
        event_ts_cet,
        lead(event_ts_cet) over (partition by device_id order by event_ts_cet) as next_event,
        lag(event_ts_cet) over (partition by device_id order by event_ts_cet) as previous_event,
        if(
            to_unix_timestamp(event_ts_cet) - to_unix_timestamp(lag(event_ts_cet) over (partition by device_id order by event_ts_cet)) <= 60*60*24
            and to_unix_timestamp(lag(event_ts_cet) over (partition by device_id order by event_ts_cet)) is not null,
            0, 1
        ) as new_session_group,
        if(source_corrected in ('unknown', 'unmarked_facebook_or_instagram'), 0, 1) as significant_source,
        source_corrected,
        campaign_corrected
    from corrected_sources
    where 1=1
),

sessions_window as 
(
    select 
        type,
        source_corrected,
        campaign_corrected,
        device_id,
        event_ts_cet,
        next_event,
        previous_event,
        sum(significant_source) over (partition by device_id order by event_ts_cet) as significant_source_window,
        sum(new_session_group) over (partition by device_id order by event_ts_cet) as timeout_source_window,
        sum(new_session_group * significant_source) over (partition by device_id order by event_ts_cet) as ultimate_window
    from session_precalc
),

sessions as 
(
    select 
        *,
        first_value(sessions_window.source_corrected) over (partition by device_id, ultimate_window order by event_ts_cet) as source,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, ultimate_window order by event_ts_cet) as campaign,
        rank() over (partition by device_id, ultimate_window order by event_ts_cet) as session_num
    from sessions_window
),

order_data AS 
(
    select
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id as order_id,
        pharmacy_landing.order.payment_method,
        from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin') as order_created_time_cet,
        date_trunc('DAY', from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) as order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        coalesce(pharmacy_landing.refund.service_fee_refund_price, 0) as service_fee_refund_price,
        sum(
            cast(
                pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity 
                - coalesce(pharmacy_landing.order_parcel_item.total_price_after_discount_price, pharmacy_landing.order_parcel_item.price * pharmacy_landing.order_parcel_item.quantity) 
            as double) 
        ) as promocode_discount,
        count(pharmacy_landing.order.id) over (partition by pharmacy_landing.order.user_email_hash order by from_utc_timestamp(pharmacy_landing.order.created, 'Europe/Berlin')) as order_num
    from {{ source('pharmacy_landing', 'order') }}
    join {{ source('pharmacy_landing', 'order_parcel') }}
        on pharmacy_landing.order.id = pharmacy_landing.order_parcel.order_id    
    join {{ source('pharmacy_landing', 'order_parcel_item') }}
        on pharmacy_landing.order_parcel_item.order_parcel_id = pharmacy_landing.order_parcel.id
        and pharmacy_landing.order_parcel_item.product_id is not null
    left join {{ source('pharmacy_landing', 'refund') }}
        on pharmacy_landing.refund.order_id = pharmacy_landing.order.id
    where 1=1
    group by 
        pharmacy_landing.order.user_email_hash,
        pharmacy_landing.order.device_id,
        pharmacy_landing.order.id,
        pharmacy_landing.order.payment_method,
        order_created_time_cet,
        order_created_date_cet,
        pharmacy_landing.order.service_fee_price,
        service_fee_refund_price
),


data_combined as
(
    select 
      event_ts_cet as session_dt,
      date_trunc('month', coalesce(event_ts_cet, order_created_time_cet)) as report_month,
      coalesce(event_ts_cet, order_created_time_cet) as report_dt,
      coalesce(sessions.source, 'no source found') as source,
      sessions.campaign,
      coalesce(sessions.device_id, order_data.device_id) as device_id,
      coalesce(order_data.user_email_hash, sessions.device_id) as combined_id,
      promocode_discount,
      order_id,
      session_num,
      order_num,
      payment_method,
      order_created_time_cet as order_dt,
      app_device_type
    from sessions
    full join order_data 
      on order_data.device_id = sessions.device_id 
      and order_data.order_created_time_cet between sessions.event_ts_cet and coalesce(sessions.next_event, sessions.event_ts_cet + interval 5 hours)
    join uid 
        on coalesce(sessions.device_id, order_data.device_id) = uid.device_id
)


select 
    source,
    report_month,
    count(cast(session_dt as string), device_id) as sessions,
    count(distinct order_id) as orders
from 
    data_combined  
group by 
    source,
    report_month
