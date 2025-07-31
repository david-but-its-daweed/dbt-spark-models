{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

with promocodes as 
(
    select 
        order_id,
        order_created_time_cet,
        device_id,
        app_device_type,
        sum(price) as discount_sum
    from {{ ref('transactions') }}
    where 1=1
        and type = 'DISCOUNT'
        and currency = 'EUR'
    group by 
        order_id,
        device_id,
        order_created_time_cet,
        app_device_type
),

sourced_promocodes as 
(
    select
        date_trunc('day', order_created_time_cet) as discount_date,
        if(lower(app_device_type) like '%web%', 'web', lower(app_device_type)) as app_device_type,
        lower(coalesce(
            max_by(paid_sources.source_corrected, paid_sources.source_dt), 
            max_by(organic_sources.source_corrected, organic_sources.source_dt),
            max_by(all_sources.source_corrected, all_sources.source_dt),
            'Unknown'
        )) as source,
        lower(coalesce(
            max_by(if(paid_sources.source_corrected like '%facebook%', paid_sources.utm_medium, ''), paid_sources.source_dt), 
            max_by(if(organic_sources.source_corrected like '%facebook%', organic_sources.utm_medium, ''), organic_sources.source_dt),
            max_by(if(all_sources.source_corrected like '%facebook%', all_sources.utm_medium, ''), all_sources.source_dt),
            'Unknown'
        )) as medium,
        lower(coalesce(
            max_by(paid_sources.campaign_corrected, paid_sources.source_dt), 
            max_by(organic_sources.campaign_corrected, organic_sources.source_dt),
            max_by(all_sources.campaign_corrected, all_sources.source_dt),
            'Unknown'
        )) as campaign,
        sum(discount_sum) as discount_sum
    from promocodes
    left join {{ ref('sources') }} AS paid_sources
        on paid_sources.device_id = promocodes.device_id
        and paid_sources.source_dt <= promocodes.order_created_time_cet
        and lower(paid_sources.source_corrected) not in ('unknown', 'organic', 'unmarked_facebook_or_instagram', 'social', 'email', 'newsletter')
    left join {{ ref('sources') }} AS organic_sources
        on organic_sources.device_id = promocodes.device_id
        and organic_sources.source_dt <= promocodes.order_created_time_cet
        and lower(organic_sources.source_corrected) <> lower('Unknown')
    left join {{ ref('sources') }} AS all_sources
        on all_sources.device_id = promocodes.device_id
        and all_sources.source_dt <= promocodes.order_created_time_cet
    group by 
        date_trunc('day', order_created_time_cet),
        if(lower(app_device_type) like '%web%', 'web', lower(app_device_type))
),

numbered_purchases as 
(
    select
        order.*, 
        rank() over (partition by order.user_email_hash order by order.created asc) as purchase_num,
        promocodes.discount_sum as discount_sum
    from 
        {{ source('pharmacy_landing', 'order') }} AS order
    left join promocodes
        on order.id = promocodes.order_id
),

first_second_purchases as 
(
    select distinct 
        first_purchases.user_email_hash,
        from_utc_timestamp(first_purchases.created, 'Europe/Berlin') as first_purchase_date_ts_cet,
        first_purchases.device_id as first_purchase_device_id,
        from_utc_timestamp(second_purchases.created, 'Europe/Berlin') as second_purchase_date_ts_cet,
        second_purchases.device_id as second_purchase_device_id,
        first_purchases.discount_sum as first_purchase_discount_sum,
        second_purchases.discount_sum as second_purchase_discount_sum
    from 
        numbered_purchases as first_purchases
    left join numbered_purchases as second_purchases
        on second_purchases.user_email_hash = first_purchases.user_email_hash
        and second_purchases.purchase_num = 2
    where
        first_purchases.purchase_num = 1
),

sources as 
(
    select 
        sources.user_email_hash,
        lower(sources.source_corrected) as source_corrected,
        lower(sources.campaign_corrected) as campaign_corrected,
        sources.partner,
        sources.source_app_device_type,
        sources.source_event_type,
        sources.source,
        sources.utm_campaign,
        if(lower(sources.source_corrected) = 'facebook', sources.medium, '') as medium,
        first_second_purchases.first_purchase_date_ts_cet,
        to_utc_timestamp(first_second_purchases.first_purchase_date_ts_cet, 'Europe/Berlin') as first_purchase_date_ts_utc,
        first_second_purchases.first_purchase_device_id,
        first_second_purchases.second_purchase_date_ts_cet,
        to_utc_timestamp(first_second_purchases.second_purchase_date_ts_cet, 'Europe/Berlin') as second_purchase_date_ts_utc,
        first_second_purchases.second_purchase_device_id,
        case
            when first_purchase_device.app_type = 'WEB' then 'web'
            when first_purchase_device.app_type = 'ANDROID' then 'android'
            when first_purchase_device.app_type = 'IOS' then 'ios'
            else 'other'
        end as first_purchase_app_device_type,
        case
            when second_purchase_device.app_type = 'WEB'  then 'web'
            when second_purchase_device.app_type = 'ANDROID' then 'android'
            when second_purchase_device.app_type = 'IOS' then 'ios'
            else 'other'
        end as second_purchase_app_device_type,
        source_ts_cet,
        source_ts_utc,
        first_second_purchases.first_purchase_discount_sum,
        first_second_purchases.second_purchase_discount_sum
    from 
        {{ ref('lndc_user_attribution') }} AS sources
    join first_second_purchases 
        on first_second_purchases.user_email_hash = sources.user_email_hash
    left join {{ source('pharmacy_landing', 'device') }} as first_purchase_device
        on first_purchase_device.id = first_second_purchases.first_purchase_device_id
    left join {{ source('pharmacy_landing', 'device') }} as second_purchase_device
        on second_purchase_device.id = first_second_purchases.second_purchase_device_id
),

installs as 
(
    select 
        lower(sources.source_corrected) as source_corrected,
        lower(sources.campaign_corrected) as campaign_corrected,
        if(lower(sources.source_corrected) = 'facebook', sources.medium, '') as medium,
        date_trunc('day', source_ts_utc) as source_day, 
        if(source_app_device_type like '%web%', 'web', source_app_device_type) as source_app_device_type,
        cast(count(distinct if(source_event_type in ('adjustInstall', 'adjustReinstall'), combined_id, null)) as bigint) as installs,
        cast(count(distinct if (source_event_type in ('externalLink'), combined_id, null)) as bigint) as visits
    from 
        {{ ref('lndc_user_attribution') }} AS sources
    group by 
        lower(sources.source_corrected),
        lower(sources.campaign_corrected),
        if(lower(sources.source_corrected) = 'facebook', sources.medium, ''),
        date_trunc('day', source_ts_utc), 
        if(source_app_device_type like '%web%', 'web', source_app_device_type)
),

ads_spends_corrected as 
(
    select
        united_spends.campaign_date_utc as partition_date,
        lower(united_spends.campaign_platform) as campaign_platform,
        case 
            when united_spends.partner = 'onfy'
            then lower(coalesce(spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict'))
            when lower(united_spends.partner) like 'ohm%' or lower(united_spends.source) like 'ohm%' then 'ohm'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.partner
        end as source_corrected,
        case 
            when united_spends.partner = 'onfy'
            then coalesce (spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict')
            when lower(united_spends.partner) like 'ohm%' or lower(united_spends.source) like 'ohm%' then 'ohm'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.partner
        end as campaign_corrected,
        if(united_spends.source = 'facebook', medium, '') as medium,
        sum(spend) as spend,
        sum(clicks) as clicks
    from 
        {{ ref('ads_spends') }} AS united_spends
    left join {{ ref("spends_campaign_corrected") }} as spends_campaigns_corrected
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

users_spends_campaigns AS (
    select distinct
        sources.user_email_hash,
        sources.first_purchase_date_ts_cet,
        sources.first_purchase_date_ts_utc,
        sources.first_purchase_device_id,
        sources.second_purchase_date_ts_cet,
        sources.second_purchase_date_ts_utc,
        sources.second_purchase_device_id,
        sources.source_ts_cet,
        sources.source_ts_utc,
        sources.source,   
        sources.medium,
        sources.source_event_type,
        sources.utm_campaign,
        sources.partner,
        sources.first_purchase_app_device_type,
        sources.second_purchase_app_device_type,
        sources.source_app_device_type,
        sources.source_corrected,
        sources.campaign_corrected,
        sources.first_purchase_discount_sum,
        sources.second_purchase_discount_sum,
        coalesce(max(ads_spends_corrected.partition_date), date_trunc('day', sources.first_purchase_date_ts_utc)) as spend_date,
        coalesce(date_trunc('day', sources.first_purchase_date_ts_utc), max(ads_spends_corrected.partition_date)) as first_purchase_date
    from 
        sources
    left join ads_spends_corrected
        on lower(ads_spends_corrected.campaign_corrected) = lower(sources.campaign_corrected)
        and lower(ads_spends_corrected.source_corrected) = lower(sources.source_corrected)
        and lower(ads_spends_corrected.medium) = lower(sources.medium)
        and ads_spends_corrected.partition_date <= date_trunc('day', sources.source_ts_utc)
    group by
        sources.user_email_hash,
        sources.first_purchase_date_ts_cet,
        sources.first_purchase_date_ts_utc,
        sources.first_purchase_device_id,
        sources.second_purchase_date_ts_cet,
        sources.second_purchase_date_ts_utc,
        sources.second_purchase_device_id,
        sources.source_ts_cet,
        sources.source_ts_utc,
        sources.source,    
        sources.medium,  
        sources.source_event_type,
        sources.utm_campaign,
        sources.partner,
        sources.first_purchase_app_device_type,
        sources.second_purchase_app_device_type,
        sources.source_app_device_type,
        sources.source_corrected,
        sources.campaign_corrected,
        sources.first_purchase_discount_sum,
        sources.second_purchase_discount_sum
),

users_by_day AS (
    select 
        users_spends_campaigns.first_purchase_date as first_purchase_date,
        users_spends_campaigns.first_purchase_app_device_type,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected,
        users_spends_campaigns.medium,
        count(distinct users_spends_campaigns.user_email_hash) as first_purchases,
        count(distinct    
            case 
                when users_spends_campaigns.second_purchase_date_ts_cet is not null
                then users_spends_campaigns.user_email_hash
                else null
            end) as second_purchases,
        sum(first_purchase_discount_sum) as first_purchase_discount_sum,   
        sum(second_purchase_discount_sum) as second_purchase_discount_sum
    from 
        users_spends_campaigns
    group by
        users_spends_campaigns.first_purchase_date,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected,
        users_spends_campaigns.medium,
        users_spends_campaigns.first_purchase_app_device_type
),

ads_spends_corrected_day AS (
    select 
        date_trunc('day', partition_date) as spend_day,
        campaign_platform,
        source_corrected,
        medium,
        campaign_corrected,
        sum(spend) as spend,
        sum(clicks) as clicks
    from 
        ads_spends_corrected
    group by 
        date_trunc('day', partition_date),
        source_corrected,
        campaign_corrected,
        medium,
        campaign_platform
)

select distinct
    coalesce(ads_spends_corrected_day.spend_day, users_by_day.first_purchase_date, install.source_day) as partition_date, 
    coalesce(ads_spends_corrected_day.source_corrected, users_by_day.source_corrected, install.source_corrected) as source,
    coalesce(ads_spends_corrected_day.campaign_corrected, users_by_day.campaign_corrected, install.campaign_corrected) as campaign,
    coalesce(ads_spends_corrected_day.medium, users_by_day.medium, install.medium) as medium,
    coalesce(ads_spends_corrected_day.campaign_platform, users_by_day.first_purchase_app_device_type, install.source_app_device_type) as platform,
    sum(coalesce(users_by_day.first_purchases, 0)) as first_purchases,
    sum(coalesce(users_by_day.second_purchases, 0)) as second_purchases, 
    sum(coalesce(ads_spends_corrected_day.spend, 0)) as spend,
    sum(coalesce(ads_spends_corrected_day.clicks, 0)) as clicks,
    sum(coalesce(install.installs, 0)) as installs,
    sum(coalesce(install.visits, 0)) as visits,
    sum(coalesce(first_purchase_discount_sum, 0)) as first_purchase_discount_sum,
    sum(coalesce(second_purchase_discount_sum, 0)) as second_purchase_discount_sum,
    sum(coalesce(discount_sum, 0)) as discount_sum
from 
    ads_spends_corrected_day
full join users_by_day
    on ads_spends_corrected_day.spend_day = users_by_day.first_purchase_date
    and lower(ads_spends_corrected_day.campaign_corrected) = lower(users_by_day.campaign_corrected)
    and lower(ads_spends_corrected_day.source_corrected) = lower(users_by_day.source_corrected)
    and lower(ads_spends_corrected_day.medium) = lower(users_by_day.medium)
    and lower(ads_spends_corrected_day.campaign_platform) = lower(users_by_day.first_purchase_app_device_type)
full join installs as install
    on ads_spends_corrected_day.spend_day = install.source_day
    and lower(ads_spends_corrected_day.campaign_corrected) = lower(install.campaign_corrected)
    and lower(ads_spends_corrected_day.source_corrected) = lower(install.source_corrected)
    and lower(ads_spends_corrected_day.medium) = lower(install.medium)
    and lower(ads_spends_corrected_day.campaign_platform) = lower(install.source_app_device_type)
full join sourced_promocodes
    on ads_spends_corrected_day.spend_day = sourced_promocodes.discount_date
    and lower(ads_spends_corrected_day.campaign_corrected) = lower(sourced_promocodes.campaign)
    and lower(ads_spends_corrected_day.source_corrected) = lower(sourced_promocodes.source)
    and lower(ads_spends_corrected_day.medium) = lower(sourced_promocodes.medium)
    and lower(ads_spends_corrected_day.campaign_platform) = lower(sourced_promocodes.app_device_type)
group by 
    coalesce(ads_spends_corrected_day.spend_day, users_by_day.first_purchase_date, install.source_day), 
    coalesce(ads_spends_corrected_day.source_corrected, users_by_day.source_corrected, install.source_corrected),
    coalesce(ads_spends_corrected_day.campaign_corrected, users_by_day.campaign_corrected, install.campaign_corrected),
    coalesce(ads_spends_corrected_day.medium, users_by_day.medium, install.medium),
    coalesce(ads_spends_corrected_day.campaign_platform, users_by_day.first_purchase_app_device_type, install.source_app_device_type)
