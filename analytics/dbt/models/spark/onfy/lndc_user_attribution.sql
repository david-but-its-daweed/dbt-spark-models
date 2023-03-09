{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true'
    }
) }}

with first_purchases as 
(
    select 
        order.user_email_hash,
        min(from_utc_timestamp(order.created, 'Europe/Berlin')) as first_purchase_date_ts_cet,
        min_by(order.device_id, order.created) as first_purchase_device_id,
        min_by(coalesce(device.os_type, 'unknown'), order.created) as device_os
    from 
      {{ source('pharmacy_landing', 'order') }} as order
    left join {{ source('pharmacy_landing', 'device') }} as device
      on order.device_id = device.id
    group by
        order.user_email_hash
),

sources as (
    select distinct
        device_events.device_id,
        device_events.type,
        device_events.payload.link,
        device_events.event_ts_cet,
        device_events.payload.referrer,
        coalesce(
            case 
                when device_events.type = 'externalLink'
                then
                    case
                        when device_events.payload.params.utm_source is not null
                        then device_events.payload.params.utm_source
                        when 
                            (device_events.payload.referrer like '%/www.google%') 
                            or (device_events.payload.referrer like '%/www.bing%')
                            or (device_events.payload.referrer like '%/search.yahoo.com%')
                            or (device_events.payload.referrer like '%/duckduckgo.com%')
                        then 'Organic'
                        when
                            (device_events.payload.referrer like '%facebook.com%') 
                            or (device_events.payload.referrer like '%instagram.com%')             
                        then 'UNMARKED_facebook_or_instagram'      
                    end
                when device_events.type in ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                then
                    case
                        when device_events.payload.utm_source = 'Unattributed' then 'Facebook'
                        when device_events.payload.utm_source is null then 'Unknown'
                        when device_events.payload.utm_source = 'Google Organic Search' then 'Organic'
                        else device_events.payload.utm_source
                    end
            end
        , 'Unknown') as source,
        case 
            when device_events.type = 'externalLink' 
            then device_events.payload.params.utm_campaign
            else device_events.payload.utm_campaign 
        end as utm_campaign,
        order.user_email_hash
    from 
        {{ source('onfy_mart', 'device_events') }} as device_events
    left join  {{ source('pharmacy_landing', 'order') }} as order
        on device_events.device_id = order.device_id
    where 
        device_events.type IN ('externalLink', 'adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
),
       
last_not_unknown as (
    select
        first_purchases.user_email_hash,
        coalesce(first_purchases.user_email_hash, paid_sources.device_id, organic_sources.device_id, all_sources.device_id) as combined_id,
        first_purchases.first_purchase_date_ts_cet,
        to_utc_timestamp(first_purchases.first_purchase_date_ts_cet, 'Europe/Berlin') as first_purchase_date_ts_utc,
        first_purchases.first_purchase_device_id,
        coalesce(max(paid_sources.event_ts_cet), max(organic_sources.event_ts_cet), max(all_sources.event_ts_cet)) as source_ts_cet,
        coalesce(
            to_utc_timestamp(max(paid_sources.event_ts_cet), 'Europe/Berlin'), 
            to_utc_timestamp(max(organic_sources.event_ts_cet), 'Europe/Berlin'), 
            to_utc_timestamp(max(all_sources.event_ts_cet), 'Europe/Berlin')
        ) as source_ts_utc,
        coalesce(
            max_by(paid_sources.source, paid_sources.event_ts_cet), 
            max_by(organic_sources.source, organic_sources.event_ts_cet),
            max_by(all_sources.source, all_sources.event_ts_cet),
            'Unknown'
        ) as source,
        coalesce(
            max_by(paid_sources.device_id, paid_sources.event_ts_cet),
            max_by(organic_sources.device_id, organic_sources.event_ts_cet),
            max_by(all_sources.device_id, all_sources.event_ts_cet)
        ) as source_device_id,       
        coalesce(
            max_by(paid_sources.type, paid_sources.event_ts_cet),
            max_by(organic_sources.type, organic_sources.event_ts_cet),
            max_by(all_sources.type, all_sources.event_ts_cet)
        ) as source_event_type,
        coalesce(
            max_by(paid_sources.utm_campaign, paid_sources.event_ts_cet), 
            max_by(organic_sources.utm_campaign, organic_sources.event_ts_cet),
            max_by(all_sources.utm_campaign, all_sources.event_ts_cet)
        ) as utm_campaign
    from 
        first_purchases
    full join sources as paid_sources
        on paid_sources.user_email_hash = first_purchases.user_email_hash
        and paid_sources.event_ts_cet <= first_purchases.first_purchase_date_ts_cet
        and paid_sources.source not in ('Unknown', 'Organic', 'UNMARKED_facebook_or_instagram', 'social', 'email', 'newsletter')
    full join sources as organic_sources
        on organic_sources.user_email_hash = first_purchases.user_email_hash
        and organic_sources.event_ts_cet <= first_purchases.first_purchase_date_ts_cet
        and organic_sources.source <> 'Unknown'
    full join sources as all_sources
        on all_sources.user_email_hash = first_purchases.user_email_hash
        and all_sources.event_ts_cet <= first_purchases.first_purchase_date_ts_cet
    group by 
        first_purchases.user_email_hash,
        first_purchases.first_purchase_date_ts_cet,
        first_purchases.first_purchase_device_id,
        coalesce(first_purchases.user_email_hash, paid_sources.device_id, organic_sources.device_id, all_sources.device_id)
),

last_not_unknown_devices_partners as (
    select distinct 
        last_not_unknown.*,
        case 
            when lower(last_not_unknown.utm_campaign) LIKE '%adcha%' then 'adchampagne'
            when lower(last_not_unknown.utm_campaign) LIKE '%rocket%' then 'rocket10'
            when lower(last_not_unknown.utm_campaign) LIKE '%whiteleads%' then 'whiteleads'
            when lower(last_not_unknown.utm_campaign) LIKE '%ohm%' then 'ohm'
            when lower(last_not_unknown.utm_campaign) LIKE '%mobihunter%' or lower(last_not_unknown.source) like '%mobihunter%' then 'mobihunter'
            else 'onfy'
        end as partner,
        case
            when first_purchase_device.app_type = 'WEB' and first_purchase_device.device_type = 'DESKTOP' then 'web_desktop'
            when first_purchase_device.app_type = 'WEB' and first_purchase_device.device_type in ('PHONE', 'TABLET') then 'web_mobile'
            when first_purchase_device.app_type = 'ANDROID' then 'android'
            when first_purchase_device.app_type = 'IOS' then 'ios'
            else first_purchase_device.app_type || '_' || first_purchase_device.device_type
        end as first_purchase_app_device_type,
        case
            when source_device.app_type = 'WEB' and source_device.device_type = 'DESKTOP' then 'web_desktop'
            when source_device.app_type = 'WEB' and source_device.device_type IN ('PHONE', 'TABLET') then 'web_mobile'
            when source_device.app_type = 'ANDROID' then 'android'
            when source_device.app_type = 'IOS' then 'ios'
            else source_device.app_type || '_' || source_device.device_type
        end as source_app_device_type
    from 
        last_not_unknown
    left join {{ source('pharmacy_landing', 'device') }} as first_purchase_device
        on first_purchase_device.id = last_not_unknown.first_purchase_device_id
    left join {{ source('pharmacy_landing', 'device') }} as source_device
        on source_device.id = last_not_unknown.source_device_id
),

users_corrected as (
    select distinct
        last_not_unknown_devices_partners.*,
        if(user_email_hash is not null, 1, 0) as is_buyer,
        lower(case 
            when last_not_unknown_devices_partners.partner = 'onfy' 
            then coalesce(pharmacy.utm_campaigns_corrected.source_corrected, last_not_unknown_devices_partners.source)
            else last_not_unknown_devices_partners.partner
        end) as source_corrected,
        case 
            when last_not_unknown_devices_partners.partner = 'onfy' 
            then coalesce(pharmacy.utm_campaigns_corrected.campaign_corrected, last_not_unknown_devices_partners.utm_campaign)
            else last_not_unknown_devices_partners.partner
        end as campaign_corrected
    from 
        last_not_unknown_devices_partners
    left join pharmacy.utm_campaigns_corrected
        on coalesce(lower(pharmacy.utm_campaigns_corrected.utm_campaign), '') = coalesce(lower(last_not_unknown_devices_partners.utm_campaign), '') 
        and coalesce(lower(pharmacy.utm_campaigns_corrected.utm_source), '') = coalesce(lower(last_not_unknown_devices_partners.source), '') 
)

select *
from users_corrected
