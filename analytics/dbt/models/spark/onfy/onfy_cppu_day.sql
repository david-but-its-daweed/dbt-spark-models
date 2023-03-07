{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true'
    }
) }}

with numbered_purchases as 
(
    select
        *, 
        rank() over (partition by user_email_hash order by created asc) as purchase_num
    from 
        {{ source('pharmacy_landing', 'order') }}
),

first_second_purchases as 
(
    select distinct 
        first_purchases.user_email_hash,
        from_utc_timestamp(first_purchases.created, 'Europe/Berlin') as first_purchase_date_ts_cet,
        first_purchases.device_id as first_purchase_device_id,
        from_utc_timestamp(second_purchases.created, 'Europe/Berlin') as second_purchase_date_ts_cet,
        second_purchases.device_id as second_purchase_device_id
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
        sources.source_corrected,
        sources.campaign_corrected,
        sources.partner,
        sources.source_app_device_type,
        sources.source_event_type,
        sources.source,
        sources.utm_campaign,
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
        source_ts_utc
    from 
        {{ source('onfy', 'lndc_user_attribution') }} as sources
    join first_second_purchases 
        on first_second_purchases.user_email_hash = sources.user_email_hash
    left join pharmacy_landing.device as first_purchase_device
        on first_purchase_device.id = first_second_purchases.first_purchase_device_id
    left join pharmacy_landing.device as second_purchase_device
        on second_purchase_device.id = first_second_purchases.second_purchase_device_id
),

installs as 
(
    select 
        source_corrected,
        campaign_corrected,
        date_trunc('day', source_ts_utc) as source_day, 
        if(source_app_device_type like '%web%', 'web', source_app_device_type) as source_app_device_type,
        cast(count(distinct if(source_event_type in ('adjustInstall', 'adjustReinstall'), combined_id, null)) as bigint) as installs,
        cast(count(distinct if (source_event_type in ('externalLink'), combined_id, null)) as bigint) as visits
    from 
        {{ source('onfy', 'lndc_user_attribution') }}
    group by 
        source_corrected,
        campaign_corrected,
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
            then lower(coalesce(pharmacy.spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict'))
            when lower(united_spends.partner) like 'ohm%' or lower(united_spends.source) like 'ohm%' then 'ohm'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.partner
        end as source_corrected,
        case 
            when united_spends.partner = 'onfy' 
            then coalesce (pharmacy.spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict')
            when lower(united_spends.partner) like 'ohm%' or lower(united_spends.source) like 'ohm%' then 'ohm'
            when united_spends.partner like '%adcha%' then 'adchampagne'
            else united_spends.partner
        end as campaign_corrected,
        sum(spend) as spend,
        sum(clicks) as clicks
    from 
        {{ source('onfy_mart', 'ads_spends') }} as united_spends
    left join pharmacy.spends_campaigns_corrected
        on lower(united_spends.campaign_name) = lower(pharmacy.spends_campaigns_corrected.campaign_name)
        and lower(united_spends.source) = lower(pharmacy.spends_campaigns_corrected.source)
    group by 
        united_spends.campaign_date_utc,
        united_spends.partner,
        source_corrected,
        campaign_corrected,
        united_spends.source,
        united_spends.campaign_name,
        united_spends.campaign_platform
),

users_spends_campaigns as 
(
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
        sources.source_event_type,
        sources.utm_campaign,
        sources.partner,
        sources.first_purchase_app_device_type,
        sources.second_purchase_app_device_type,
        sources.source_app_device_type,
        sources.source_corrected,
        sources.campaign_corrected,
        coalesce(max(ads_spends_corrected.partition_date), date_trunc('day', sources.first_purchase_date_ts_utc)) as spend_date,
        coalesce(date_trunc('day', sources.first_purchase_date_ts_utc), max(ads_spends_corrected.partition_date)) as first_purchase_date
    from 
        sources
    left join ads_spends_corrected
        on lower(ads_spends_corrected.campaign_corrected) = lower(sources.campaign_corrected)
        and lower(ads_spends_corrected.source_corrected) = lower(sources.source_corrected)
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
        sources.source_event_type,
        sources.utm_campaign,
        sources.partner,
        sources.first_purchase_app_device_type,
        sources.second_purchase_app_device_type,
        sources.source_app_device_type,
        sources.source_corrected,
        sources.campaign_corrected
),

users_by_day as 
(
    select 
        users_spends_campaigns.first_purchase_date as first_purchase_date,
        users_spends_campaigns.first_purchase_app_device_type,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected,
        count(distinct users_spends_campaigns.user_email_hash) as first_purchases,
        count(distinct    
            case 
                when users_spends_campaigns.second_purchase_date_ts_cet is not null
                then users_spends_campaigns.user_email_hash
                else null
            end) as second_purchases
    from 
        users_spends_campaigns
    group by
        users_spends_campaigns.first_purchase_date,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected,
        users_spends_campaigns.first_purchase_app_device_type
),

ads_spends_corrected_day as 
(
    select 
        date_trunc('day', partition_date) as spend_day,
        campaign_platform,
        source_corrected,
        campaign_corrected,
        sum(spend) as spend,
        sum(clicks) as clicks
    from 
        ads_spends_corrected
    group by 
        date_trunc('day', partition_date),
        source_corrected,
        campaign_corrected,
        campaign_platform
)

select distinct
    coalesce(ads_spends_corrected_day.spend_day, users_by_day.first_purchase_date) as partition_date, 
    coalesce(ads_spends_corrected_day.source_corrected, users_by_day.source_corrected) as source,
    coalesce(ads_spends_corrected_day.campaign_corrected, users_by_day.campaign_corrected) as campaign,
    coalesce(ads_spends_corrected_day.campaign_platform, users_by_day.first_purchase_app_device_type) as platform,
    coalesce(users_by_day.first_purchases, 0) as first_purchases,
    coalesce(users_by_day.second_purchases, 0) as second_purchases, 
    coalesce(ads_spends_corrected_day.spend, 0) as spend,
    coalesce(ads_spends_corrected_day.clicks, 0) as clicks,
    coalesce(installs.installs, 0) as installs,
    coalesce(installs.visits, 0) as visits
from 
    ads_spends_corrected_day
full outer join users_by_day
    on ads_spends_corrected_day.spend_day = users_by_day.first_purchase_date
    and lower(ads_spends_corrected_day.campaign_corrected) = lower(users_by_day.campaign_corrected)
    and lower(ads_spends_corrected_day.source_corrected) = lower(users_by_day.source_corrected)
    and lower(ads_spends_corrected_day.campaign_platform) = lower(users_by_day.first_purchase_app_device_type)
left join installs
    on ads_spends_corrected_day.spend_day = installs.source_day
    and lower(ads_spends_corrected_day.campaign_corrected) = lower(installs.campaign_corrected)
    and lower(ads_spends_corrected_day.source_corrected) = lower(installs.source_corrected)
    and lower(ads_spends_corrected_day.campaign_platform) = lower(installs.source_app_device_type)  
