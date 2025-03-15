{{ config(
    schema='onfy',
    materialized='table',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_overwrite': 'true'
    }
) }}

-----------------------------------------
-- забираю id пользователей для добавления user_email_hash
-----------------------------------------
with uid as
(
    select
        device_id,
        app_device_type,
        if(app_device_type like 'web%', 'web', app_device_type) as app_type,
        min_by(user_email_hash, min_purchase_date) as user_email_hash
    from {{source('onfy_mart', 'devices_mart')}}
    where 1=1
    group by 
        device_id,
        app_device_type
),

-----------------------------------------
-- сорсы + определяю партнера и следующую сессию в окне
-----------------------------------------
corrected_sources as 
(
    select distinct
        *,
        date_trunc('day', source_dt) as source_date,
        case 
            when lower(campaign_corrected) like '%adcha%' then 'adchampagne'
            when lower(campaign_corrected) like '%mobihunter%' then 'mobihunter'
            when lower(campaign_corrected) like 'ohm%' then 'ohm'
            when lower(campaign_corrected) like 'rocket10%' then 'rocket10'
            else 'onfy'
        end as partner,
        coalesce(next_source_dt, source_dt + interval 168 hours) as window_next_session
    from {{source('onfy', 'sources')}} 
    where 1=1

),

-----------------------------------------
-- заказы (только евро!)
-----------------------------------------
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


-----------------------------------------
-- определяю окна для смены источника:
-- 1. по типу источника (значимый платный / значимый бесплатный / незначимый)
-- 2. по времени, прошедшему с предыдущей сессии (пока — 168 и 7 часов)

-- доопределяю оставшиеся источники
-- джойню ордера
-----------------------------------------
session_precalc as 
(
    select distinct
        type,
        coalesce(corrected_sources.device_id, order_data.device_id) as device_id,
        coalesce(source_date, order_created_date_cet) as source_date,
        coalesce(source_dt, order_created_time_cet) as source_dt,
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
        ) as new_session_group_7,
        if(
            to_unix_timestamp(coalesce(source_dt, order_created_date_cet)) - to_unix_timestamp(lag(coalesce(source_dt, order_created_date_cet)) 
                over (partition by coalesce(corrected_sources.device_id, order_data.device_id) order by coalesce(source_dt, order_created_date_cet))) <= 60*60*24
            and to_unix_timestamp(lag(coalesce(source_dt, order_created_date_cet)) 
                over (partition by coalesce(corrected_sources.device_id, order_data.device_id) order by coalesce(source_dt, order_created_date_cet))) is not null,
            0, 1
        ) as new_session_group_1,
        if(lower(source_corrected) in ('unknown', 'unmarked_facebook_or_instagram', 'social', 'e-rezept', 'marketing_newsletter', 'newsletter', 'email', '') or lower(source_corrected) is null
            or (lower(source_corrected) = 'organic' and os_type in ('android', 'ios')), 0, 1) as significant_source,
        if(lower(source_corrected) not in ('unknown', 'e-rezept'), 0, 1) as second_significant_source,
        if(lower(source_corrected) in ('unknown', 'unmarked_facebook_or_instagram', 'social', 'e-rezept', 'marketing_newsletter', 'newsletter', 'email', '', 'awin') or lower(source_corrected) is null
            or (lower(source_corrected) = 'organic' and os_type in ('android', 'ios')), 0, 1) as significant_source_awin,
        case
            when lower(source_corrected) like '%google%' then 'google'
            else source_corrected
        end as source_corrected,
        split_part(
            case 
                when lower(source_corrected) like '%tiktok%' then 'tiktok' 
            --    when lower(campaign_corrected) like 'ohm%' then 'ohm'
                else campaign_corrected
            end
            , " ", 1) as campaign_corrected,
        if(lower(source_corrected) = 'facebook', utm_medium, '') as utm_medium,
        landing_page,
        parse_url(concat("https://onfy.de", landing_page), 'QUERY', 'gclid') as gclid,
        cast(regexp_extract(landing_page, 'artikel/(\d+)/', 1) as string) as landing_pzn,
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
        and order_data.order_created_time_cet between corrected_sources.source_dt and window_next_session
    where 1=1
),

-----------------------------------------
-- имитирую партицию, по которой буду затем делать окно.
-- партиция нужна, чтобы "протягивать" нужный источник на последующие сессии
-----------------------------------------
sessions_window as 
(
    select 
        *,
        sum(significant_source) over (partition by device_id order by source_dt) as significant_source_window,
        sum(new_session_group_7) over (partition by device_id order by source_dt) as timeout_source_window,
        sum(array_max(array(new_session_group_7, significant_source))) over (partition by device_id order by source_dt) as ultimate_window,
        sum(array_max(array(new_session_group_1, significant_source))) over (partition by device_id order by source_dt) as ultimate_window_1,
        sum(array_max(array(new_session_group_7, significant_source_awin))) over (partition by device_id order by source_dt) as ultimate_window_awin,
        sum(array_max(array(new_session_group_7, second_significant_source))) over (partition by device_id order by source_dt) as second_ultimate_window
    from session_precalc
),

-----------------------------------------
-- забираем значимый источник в окне
-----------------------------------------
sessions as 
(
    select 
        type,
        device_id,
        source_dt,
        source_date,
        next_event,
        previous_event,
        new_session_group_7,
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
    
        first_value(sessions_window.source_dt) over (partition by device_id, if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt) as attributed_session_dt,
        date_trunc('day', first_value(sessions_window.source_dt) over (partition by device_id,  if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt)) as attributed_session_date,
        first_value(sessions_window.source_corrected) over (partition by device_id,  if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt) as source,
        first_value(sessions_window.campaign_corrected) over (partition by device_id,  if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt) as campaign,
        first_value(sessions_window.utm_medium) over (partition by device_id,  if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt) as medium,
        first_value(sessions_window.gclid) over (partition by device_id,  if(coalesce(promocode_discount, 0) > 0, ultimate_window_awin, ultimate_window) order by source_dt) as attributed_gclid,
    
        first_value(sessions_window.source_dt) over (partition by device_id, ultimate_window_1 order by source_dt) as attributed_session_dt_1,
        date_trunc('day', first_value(sessions_window.source_dt) over (partition by device_id, ultimate_window_1 order by source_dt)) as attributed_session_date_1,
        first_value(sessions_window.source_corrected) over (partition by device_id, ultimate_window_1 order by source_dt) as source_1,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, ultimate_window_1 order by source_dt) as campaign_1,
        first_value(sessions_window.utm_medium) over (partition by device_id, ultimate_window_1 order by source_dt) as medium_1, 
    
        first_value(sessions_window.source_dt) over (partition by device_id, second_ultimate_window order by source_dt) as second_attributed_session_dt,
        first_value(sessions_window.source_corrected) over (partition by device_id, second_ultimate_window order by source_dt) as second_source,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, second_ultimate_window order by source_dt) as second_campaign,
        first_value(sessions_window.utm_medium) over (partition by device_id, second_ultimate_window order by source_dt) as second_medium,
    
        first_value(sessions_window.source_corrected) over (partition by device_id, significant_source_window order by source_dt) as source_significant,
        first_value(sessions_window.campaign_corrected) over (partition by device_id, significant_source_window order by source_dt) as campaign_significant,
        first_value(sessions_window.utm_medium) over (partition by device_id, significant_source_window order by source_dt) as medium_significant,
    
        landing_page,
        gclid,
        landing_pzn
    from sessions_window
),

-----------------------------------------
-- рекламные расходы с преобразованиями (в частности, сплит по пробелу)
-----------------------------------------
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
            when lower(united_spends.source) like '%tiktok%' or lower(united_spends.source) like '%tik tok%' then 'tiktok'
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) not like '%google%'
            then lower(coalesce(spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict'))
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) like '%google%'
            then 'google'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.source
        end as source_corrected,
        case 
            when lower(united_spends.source) like '%tiktok%' or lower(united_spends.source) like '%tik tok%' then 'tiktok'
            when united_spends.partner = 'onfy' 
            then split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1)
            when united_spends.source = 'idealo' then 'idealo'
            else united_spends.campaign_name
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
        case 
            when split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1) = 'Onfy_UA_PerfMax' then 'android'
            when split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1) = 'Shopping_new' then 'web'            
            else lower(united_spends.campaign_platform)
        end,
        case 
            when lower(united_spends.source) like '%tiktok%' or lower(united_spends.source) like '%tik tok%' then 'tiktok'
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) not like '%google%'
            then lower(coalesce(spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict'))
            when united_spends.partner = 'onfy' and lower(coalesce(spends_campaigns_corrected.source, united_spends.source)) like '%google%'
            then 'google'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.source
        end,
        case 
            when lower(united_spends.source) like '%tiktok%' or lower(united_spends.source) like '%tik tok%' then 'tiktok'
            when united_spends.partner = 'onfy' 
            then split_part(coalesce(spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict'), " ", 1)
            when united_spends.source = 'idealo' then 'idealo'
            else united_spends.campaign_name
        end,
        if(united_spends.source = 'facebook', medium, '')
),

-----------------------------------------
-- забираем только дистинкты, а также определяем источник иерархически
-----------------------------------------
filtered_data as
(
    select distinct
        source_dt as session_dt,
        sessions.source_date as session_date,
        attributed_session_date,
        attributed_session_dt,
        attributed_session_date_1,
        attributed_session_dt_1,
        coalesce(sessions.source, sessions.second_source, sessions.source_corrected, 'unknown') as source,
        sessions.source_significant,
        coalesce(sessions.campaign, sessions.second_campaign, sessions.campaign_corrected, 'unknown') as campaign,
        sessions.campaign_significant,
        coalesce(sessions.medium, sessions.second_medium, sessions.utm_medium) as medium,
        coalesce(sessions.source_1, sessions.second_source, sessions.source_corrected, 'unknown') as source_1,
        coalesce(sessions.campaign_1, sessions.second_campaign, sessions.campaign_corrected, 'unknown') as campaign_1,
        coalesce(sessions.medium_1, sessions.second_medium, sessions.utm_medium) as medium_1,
        sessions.medium_significant,
        sessions.ultimate_window,
        sessions.significant_source_window,
        sessions.timeout_source_window,
        sessions.source_corrected,
        sessions.campaign_corrected,
        sessions.significant_source,
        sessions.utm_medium,
        sessions.timeout_source_window,
        sessions.device_id,
        coalesce(sessions.user_email_hash, sessions.device_id) as combined_id,
        promocode_discount,
        order_id,
        purchase_num,
        gmv_initial,
        gross_profit_initial,
        order_created_time_cet as order_dt,
        order_created_date_cet as order_date,
        app_device_type,
        app_type,
        sessions.user_email_hash,
        attributed_gclid,
        landing_page,
        gclid,
        landing_pzn
    from sessions as sessions
    join uid 
        on sessions.device_id = uid.device_id
),

-----------------------------------------
-- делаю два джойна адспендов:
-- 1. к сессиям, за которые мы непосредственно заплатили
-- 2. к атрибуцированным сессиям (размываем косты в течение окна)

-- также определяем [атрибуцированную] стоимость ордера (делим все косты с кампании за дату на количество ордеров с нее)
-----------------------------------------
data_combined as 
(
    select distinct
        session_dt,
        date_trunc('month', coalesce(session_dt, ads_spends.partition_date)) as report_month,
        coalesce(sessions.session_date, ads_spends.partition_date) as report_date,
        session_date,
        attributed_session_date,
        attributed_session_date_1 as attributed_session_date_1d,
        sessions.source,
        rank() over (partition by sessions.device_id order by session_dt) as session_num,
        sessions.source_significant,
        sessions.campaign,
        sessions.campaign_significant,
        sessions.medium,
        sessions.medium_significant,
        sessions.ultimate_window,
        sessions.significant_source_window,
        sessions.timeout_source_window,
        sessions.source_corrected,
        sessions.campaign_corrected,
        sessions.significant_source,
        sessions.utm_medium,
        attributed_session_dt,
        attributed_session_dt_1 as attributed_session_dt_1d,
        ads_spends.source_corrected as ads_source,
        ads_spends.campaign_corrected as ads_campaign,
        ads_spends.medium as ads_medium,
        ads_spends.campaign_platform,
        coalesce(sessions.source, source_significant, sessions.source_corrected, ads_spends.source_corrected) as report_source,
        coalesce(sessions.campaign, campaign_significant, sessions.campaign_corrected, ads_spends.campaign_corrected) as report_campaign,
        coalesce(sessions.medium, medium_significant, sessions.utm_medium, ads_spends.medium) as report_medium,
        sessions.timeout_source_window,
        sessions.device_id,
        combined_id,
        user_email_hash,
        promocode_discount,
        order_id,
        session_num,
        purchase_num,
        gmv_initial,
        gross_profit_initial,
        order_dt,
        order_date,
        app_device_type,
        source_1 as source_1d,
        medium_1 as medium_1d,
        campaign_1 as campaign_1d,
        coalesce(ads_spends.spend, 0) as total_spend,
        coalesce(ads_spends_attributed.spend, 0) as attributed_spend,
        coalesce(ads_spends_attributed_1.spend, 0) as attributed_spend_1d,
        coalesce(sessions.app_type, ads_spends.campaign_platform) as report_app_type,
        attributed_gclid,
        landing_page,
        gclid,
        landing_pzn
    from filtered_data as sessions
    full join ads_spends 
        on date_trunc('day', ads_spends.partition_date) = sessions.session_date
        and lower(ads_spends.campaign_corrected) = lower(sessions.campaign_corrected)
        and lower(ads_spends.source_corrected) = lower(sessions.source_corrected)
        and lower(ads_spends.medium) = lower(sessions.utm_medium)
        and ads_spends.campaign_platform = sessions.app_type
    left join ads_spends as ads_spends_attributed
        on date_trunc('day', ads_spends_attributed.partition_date) = sessions.attributed_session_date
        and lower(ads_spends_attributed.campaign_corrected) = lower(sessions.campaign)
        and lower(ads_spends_attributed.source_corrected) = lower(sessions.source)
        and lower(ads_spends_attributed.medium) = lower(sessions.medium)
        and ads_spends_attributed.campaign_platform = sessions.app_type
    left join ads_spends as ads_spends_attributed_1
        on date_trunc('day', ads_spends_attributed_1.partition_date) = sessions.attributed_session_date_1
        and lower(ads_spends_attributed_1.campaign_corrected) = lower(sessions.campaign_1)
        and ads_spends_attributed_1.campaign_platform = sessions.app_type
        and lower(ads_spends_attributed_1.source_corrected) = lower(sessions.source_1)
        and lower(ads_spends_attributed_1.medium) = lower(sessions.medium_1) 
),

-----------------------------------------
-- считаю распределенный рекламный кост
-----------------------------------------
spend_distributed as 
(
    select
        session_dt,
        report_month,
        report_date,
        session_date,
        source,
        session_num,
        source_significant,
        campaign,
        campaign_significant,
        medium,
        medium_significant,
        source_1d,
        medium_1d,
        campaign_1d,
        ultimate_window,
        significant_source_window,
        timeout_source_window,
        source_corrected,
        campaign_corrected,
        significant_source,
        utm_medium,
        attributed_session_dt,
        attributed_session_dt_1d,
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
        user_email_hash,
        promocode_discount,
        order_id,
        session_num,
        purchase_num,
        gmv_initial,
        gross_profit_initial,
        order_dt,
        order_date,
        app_device_type,
        total_spend,
        attributed_spend,
        attributed_spend_1d,
        report_app_type,
        count(*) over (partition by session_date, campaign_corrected, source_corrected, 
        utm_medium) as campaign_sessions,
        count(order_id) over (partition by order_date, campaign_corrected, source_corrected, 
        utm_medium) as campaign_purchases,
        count(*) over (partition by attributed_session_date, campaign, source, 
        medium) as attributed_campaign_sessions,
        count(order_id) over (partition by attributed_session_date, campaign, source,
        medium) as attributed_campaign_purchases,
        count(if(purchase_num = 1, order_id, null)) over (partition by attributed_session_date, campaign, source,
        medium) as attributed_campaign_first_purchases,
        count(*) over (partition by attributed_session_date_1d, campaign_1d, source_1d, 
        medium_1d) as attributed_campaign_sessions_1d,
        count(order_id) over (partition by attributed_session_date_1d, campaign_1d, source_1d,
        medium_1d) as attributed_campaign_purchases_1d,
        attributed_gclid,
        landing_page,
        gclid,
        landing_pzn
    from data_combined
)


select 
    session_dt,
    attributed_session_dt,
    report_month,
    report_date,
    CAST(report_date AS DATE) as partition_date,
    source,
    source_significant,
    source_corrected as session_source,
    campaign,
    campaign_significant,
    campaign_corrected as session_campaign,
    medium,
    medium_significant,
    utm_medium as session_medium,
    source_1d,
    medium_1d,
    campaign_1d,
    ultimate_window,
    significant_source_window,
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
    user_email_hash as order_user_email_hash,
    first_value(source) over (partition by user_email_hash order by session_dt) as first_user_source,
    first_value(campaign) over (partition by user_email_hash order by session_dt) as first_user_campaign,
    first_value(medium) over (partition by user_email_hash order by session_dt) as first_user_medium,
    min(session_dt) over (partition by user_email_hash order by session_dt) as first_user_session_dt,
    min(order_dt) over (partition by user_email_hash order by order_dt) as first_order_dt,
    promocode_discount,
    order_id,
    gmv_initial,
    gross_profit_initial,
    session_num,
    purchase_num,
    order_dt,
    order_date,
    app_device_type,
    total_spend,
    attributed_spend,
    report_app_type,
    campaign_sessions,
    campaign_purchases,
    attributed_campaign_sessions,
    attributed_campaign_purchases,
    attributed_campaign_sessions_1d,
    attributed_campaign_purchases_1d,
    total_spend / if(source_corrected <> 'unknown' and campaign_corrected <> 'unknown', campaign_sessions, 1) as session_spend,
    if(order_id is not null, total_spend, 0) / if(source_corrected <> 'unknown' and campaign_corrected <> 'unknown', campaign_purchases, 1) as order_session_spend,
    attributed_spend / if(source <> 'unknown' and campaign <> 'unknown', attributed_campaign_sessions, 1) as attributed_session_spend,
    if(order_id is not null, attributed_spend, 0) / if(source <> 'unknown' and campaign <> 'unknown', attributed_campaign_purchases, 1) as attributed_order_spend,
    if(order_id is not null and purchase_num = 1, attributed_spend, 0) / if(source <> 'unknown' and campaign <> 'unknown' and purchase_num = 1, attributed_campaign_first_purchases, 1) as attributed_first_order_spend,
    attributed_spend_1d / if(source_1d <> 'unknown' and campaign_1d <> 'Unknown', attributed_campaign_sessions_1d, 1) as attributed_session_spend_1d,
    if(order_id is not null, attributed_spend_1d, 0) / if(source_1d <> 'unknown' and campaign_1d <> 'unknown', attributed_campaign_purchases_1d, 1) as attributed_order_spend_1d,
    attributed_gclid,
    landing_page,
    gclid,
    landing_pzn
from spend_distributed
WHERE report_date IS NOT NULL
DISTRIBUTE BY partition_date
