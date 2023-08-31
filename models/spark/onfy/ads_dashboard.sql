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
    from {{source('onfy_mart', 'devices_mart')}}
    where 1=1
        and is_bot = False
    group by 
        device_id,
        app_device_type
),

corrected_sources as 
(
    select *
    from {{source('onfy', 'sources')}} 
),

order_data as 
(
    select 
        user_email_hash,
        device_id,
        order_id,
        order_created_time_cet,
        date_trunc('day', order_created_time_cet) as order_created_date_cet,
        purchase_num,
        sum(if(type = 'DISCOUNT', price, 0)) as promocode_discount,
        sum(gmv_initial) as gmv_initial,
        sum(gross_profit_initial) as gross_profit_initial
    from {{source('onfy', 'transactions')}}
    where 1=1
        and currency = 'EUR'
    group by 
        user_email_hash,
        device_id,
        order_id,
        order_created_time_cet,
        purchase_num
),

session_precalc as 
(
    select 
        type,
        coalesce(corrected_sources.device_id, order_data.device_id) as device_id,
        coalesce(source_dt, order_created_date_cet) as source_dt,
        lead(coalesce(source_dt, order_created_date_cet)) over (partition by coalesce(corrected_sources.device_id, order_data.device_id)
            order by coalesce(source_dt, order_created_date_cet)) as next_event,
        lag(coalesce(source_dt, order_created_date_cet)) over (partition by coalesce(corrected_sources.device_id, order_data.device_id)
            order by coalesce(source_dt, order_created_date_cet)) as previous_event,
        if(
            to_unix_timestamp(coalesce(source_dt, order_created_date_cet)) - to_unix_timestamp(lag(coalesce(source_dt, order_created_date_cet)) 
                over (partition by coalesce(corrected_sources.device_id, order_data.device_id) order by coalesce(source_dt, order_created_date_cet))) <= 60*60*24*7
            and to_unix_timestamp(lag(coalesce(source_dt, order_created_date_cet)) 
                over (partition by coalesce(corrected_sources.device_id, order_data.device_id) order by coalesce(source_dt, order_created_date_cet))) is not null,
            0, 1
        ) as new_session_group,
        if(source_corrected in ('unknown', 'unmarked_facebook_or_instagram', 'social'), 0, 1) as significant_source,
        if(lower(source_corrected) like '%google%', 'google', source_corrected) as source_corrected,
        if(lower(source_corrected) like '%tok%', "", campaign_corrected) as campaign_corrected,
        if(lower(source_corrected) = 'facebook', utm_medium, '') as utm_medium,
        user_email_hash,
        order_id,
        order_created_time_cet,
        order_created_date_cet,
        purchase_num,
        promocode_discount,
        gmv_initial,
        gross_profit_initial
    from corrected_sources
    full join order_data 
        on order_data.device_id = corrected_sources.device_id 
        and order_data.order_created_time_cet between corrected_sources.source_dt and coalesce(corrected_sources.next_source_dt, corrected_sources.source_dt + interval 168 hours)
    where 1=1
),

sessions_window as 
(
    select 
        *,
        sum(significant_source) over (partition by device_id order by source_dt) as significant_source_window,
        sum(new_session_group) over (partition by device_id order by source_dt) as timeout_source_window,
        sum(new_session_group * significant_source) over (partition by device_id order by source_dt) as ultimate_window
    from session_precalc
),

sessions as 
(
    select 
        type,
        device_id,
        source_dt,
        next_event,
        previous_event,
        new_session_group,
        significant_source,
        source_corrected,
        campaign_corrected,
        utm_medium,
        user_email_hash,
        order_id,
        order_created_time_cet,
        order_created_date_cet,
        purchase_num,
        promocode_discount,
        gmv_initial,
        gross_profit_initial,
        significant_source_window,
        timeout_source_window,
        ultimate_window,
        first_value(sessions_window.source_dt) over (partition by device_id, ultimate_window order by source_dt) as attributed_session_dt,
        first_value(sessions_window.source_corrected) over (partition by device_id, ultimate_window order by source_dt) as source,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, ultimate_window order by source_dt) as campaign,
        first_value(sessions_window.utm_medium) over (partition by device_id, ultimate_window order by source_dt) as medium,
        first_value(sessions_window.source_corrected) over (partition by device_id, significant_source_window order by source_dt) as source_significant,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, significant_source_window order by source_dt) as campaign_significant,
        first_value(sessions_window.utm_medium) over (partition by device_id, significant_source_window order by source_dt) as medium_significant
    from sessions_window
),


ads_spends as
(
    select
        united_spends.campaign_date_utc as partition_date,
        case 
            when split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1) = 'Onfy_UA_PerfMax' then 'android'
            when split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1) = 'Shopping_new' then 'web'            
            else lower(united_spends.campaign_platform)
        end as campaign_platform,
        case 
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) not like '%google%'
            then lower(coalesce(spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict'))
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) like '%google%'
            then 'google'
            when lower(united_spends.partner) like 'ohm%' or lower(united_spends.source) like 'ohm%' then 'ohm'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.partner
        end as source_corrected,
        case 
            when united_spends.partner = 'onfy' 
            then split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1)
            when united_spends.source = 'idealo' then 'idealo'
            when united_spends.partner = 'ohm%' then null
        end as campaign_corrected,
        if(united_spends.source = 'facebook', medium, '') as medium,
        sum(spend) as spend,
        sum(clicks) as clicks
    from {{source('onfy_mart', 'ads_spends')}} as united_spends
    left join {{ref("spends_campaign_corrected")}} as spends_campaigns_corrected
        on lower(united_spends.campaign_name) = lower(spends_campaigns_corrected.campaign_name)
        and lower(united_spends.source) = lower(spends_campaigns_corrected.source)
    group by 
        united_spends.campaign_date_utc,
        united_spends.partner,
        source_corrected,
        campaign_corrected,
        united_spends.source,
        united_spends.campaign_name,
        united_spends.campaign_platform,
        if(united_spends.source = 'facebook', medium, '')
),

data_combined as
(
    select 
        source_dt as session_dt,
        date_trunc('month', coalesce(source_dt, ads_spends.partition_date)) as report_month,
        coalesce(date_trunc('day', source_dt), ads_spends.partition_date) as report_date,
        coalesce(sessions.source, 'direct') as source,
        rank() over (partition by sessions.device_id, ultimate_window order by source_dt) as session_num,
        sessions.source_significant,
        sessions.campaign,
        sessions.campaign_significant,
        sessions.medium,
        sessions.medium_significant,
        sessions.ultimate_window,
        sessions.significant_source_window,
        sessions.source_corrected,
        sessions.campaign_corrected,
        sessions.utm_medium,
        sessions.attributed_session_dt,
        ads_spends.source_corrected as ads_source,
        ads_spends.campaign_corrected as ads_campaign,
        ads_spends.medium as ads_medium,
        ads_spends.campaign_platform,
        coalesce(sessions.source, source_significant, sessions.source_corrected, ads_spends.source_corrected) as report_source,
        coalesce(sessions.campaign, campaign_significant, sessions.campaign_corrected, ads_spends.campaign_corrected) as report_campaign,
        coalesce(sessions.medium, medium_significant, sessions.utm_medium, ads_spends.medium) as report_medium,
        sessions.timeout_source_window,
        sessions.device_id,
        coalesce(sessions.user_email_hash, sessions.device_id) as combined_id,
        promocode_discount,
        order_id,
        session_num,
        purchase_num,
        gmv_initial,
        gross_profit_initial,
        order_created_time_cet as order_dt,
        order_created_date_cet as order_date,
        app_device_type,
        coalesce(ads_spends.spend, 0) as total_spend,
        coalesce(ads_spends_attributed.spend, 0) as attributed_spend,
        coalesce(if(app_device_type like 'web%', 'web', app_device_type), ads_spends.campaign_platform) as report_app_type,
        count(*) over (partition by date_trunc('day', sessions.source_dt), campaign_significant, source_significant, 
        utm_medium, if(app_device_type like 'web%', 'web', app_device_type)) as campaign_sessions,
        count(order_id) over (partition by order_created_date_cet, campaign_significant, source_significant, 
        utm_medium, if(app_device_type like 'web%', 'web', app_device_type)) as campaign_purchases,
        count(*) over (partition by date_trunc('day', sessions.attributed_session_dt), campaign, source, 
        sessions.medium, if(app_device_type like 'web%', 'web', app_device_type)) as attributed_campaign_sessions,
        count(order_id) over (partition by date_trunc('day', sessions.attributed_session_dt), campaign, source,
        sessions.medium, if(app_device_type like 'web%', 'web', app_device_type)) as attributed_campaign_purchases
    from sessions as sessions
    join uid 
        on sessions.device_id = uid.device_id
    full join ads_spends 
        on date_trunc('day', ads_spends.partition_date) = date_trunc('day', sessions.source_dt)
        and lower(ads_spends.campaign_corrected) = lower(sessions.campaign_corrected)
        and lower(ads_spends.campaign_platform) = if(app_device_type like 'web%', 'web', app_device_type)
        and lower(ads_spends.source_corrected) = lower(sessions.source_corrected)
        and lower(ads_spends.medium) = lower(sessions.utm_medium)
    left join ads_spends as ads_spends_attributed
        on date_trunc('day', ads_spends_attributed.partition_date) = date_trunc('day', sessions.attributed_session_dt)
        and lower(ads_spends_attributed.campaign_corrected) = lower(sessions.campaign)
        and lower(ads_spends_attributed.campaign_platform) = if(app_device_type like 'web%', 'web', app_device_type)
        and lower(ads_spends_attributed.source_corrected) = lower(sessions.source)
        and lower(ads_spends_attributed.medium) = lower(sessions.medium)

)



select distinct
    session_dt,
    attributed_session_dt,
    report_month,
    report_date,
    source,
    source_significant,
    campaign,
    campaign_significant,
    medium,
    medium_significant,
    ultimate_window,
    significant_source_window,
    source_corrected,
    campaign_corrected,
    utm_medium,
    ads_source,
    ads_campaign,
    ads_medium,
    campaign_platform,
    report_source,
    report_campaign,
    report_medium,
    timeout_source_window,
    device_id,
    combined_id,
    promocode_discount,
    order_id,
    gmv_initial,
    gross_profit_initial,
    session_num,
    purchase_num,
    order_dt,
    order_date,
    app_device_type,
    total_spend as total_spend,
    attributed_spend,
    report_app_type,
    campaign_sessions,
    campaign_purchases,
    attributed_campaign_sessions,
    attributed_campaign_purchases,
    total_spend / if(source_corrected is not null or campaign_corrected is not null, campaign_sessions, 1) as session_spend,
    if(order_id is not null, total_spend, 0) / if(source_corrected is not null or campaign_corrected is not null, campaign_purchases, 1) as order_session_spend,
    attributed_spend / if(source is not null or campaign is not null, attributed_campaign_sessions, 1) as attributed_session_spend,
    if(order_id is not null, attributed_spend, 0) / if(source is not null or campaign is not null, attributed_campaign_purchases, 1) as attributed_order_spend
from data_combined
