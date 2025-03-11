{{ config(
    schema='onfy',
    materialized='table',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'alerts_channel': '#onfy-etl-monitoring',
      'priority_weight': '150',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_overwrite': 'true',
      'bigquery_known_gaps': [
        '2021-09-23', '2021-10-15', '2021-10-20', '2021-09-29', '2021-11-15', '2021-10-27',
        '2021-10-16', '2021-10-10', '2021-10-01', '2021-10-26', '2021-10-28', '2021-10-14', 
        '2021-10-13', '2021-10-19', '2021-09-19', '2021-10-12', '2021-10-23', '2021-09-27', 
        '2021-11-02', '2021-11-04', '2021-11-06', '2021-11-05', '2021-11-10', '2021-10-08', 
        '2021-10-17', '2021-10-24', '2021-11-07', '2021-09-28', '2021-11-03', '2021-10-25', 
        '2021-10-18', '2021-10-06', '2021-09-26', '2021-09-21', '2021-09-16', '2021-10-02', 
        '2021-09-20', '2021-09-24', '2021-10-11', '2021-10-03', '2021-09-13', '2021-11-08', 
        '2021-11-09', '2021-10-30', '2021-09-15', '2021-09-25', '2021-10-21', '2021-10-31', 
        '2021-09-30', '2021-10-04'
      ]
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
        sources.*,
        order.user_email_hash
    from 
        {{ source('onfy', 'sources') }} as sources
    left join {{ source('pharmacy_landing', 'order') }}
        on sources.device_id = order.device_id
),
       
last_not_unknown as (
    select
        first_purchases.user_email_hash,
        coalesce(first_purchases.user_email_hash, paid_sources.device_id, organic_sources.device_id, all_sources.device_id) as combined_id,
        first_purchases.first_purchase_date_ts_cet,
        to_utc_timestamp(first_purchases.first_purchase_date_ts_cet, 'Europe/Berlin') as first_purchase_date_ts_utc,
        first_purchases.first_purchase_device_id,
        coalesce(max(paid_sources.source_dt), max(organic_sources.source_dt), max(all_sources.source_dt)) as source_ts_cet,
        coalesce(
            to_utc_timestamp(max(paid_sources.source_dt), 'Europe/Berlin'), 
            to_utc_timestamp(max(organic_sources.source_dt), 'Europe/Berlin'), 
            to_utc_timestamp(max(all_sources.source_dt), 'Europe/Berlin')
        ) as source_ts_utc,
        coalesce(
            max_by(paid_sources.source_corrected, paid_sources.source_dt), 
            max_by(organic_sources.source_corrected, organic_sources.source_dt),
            max_by(all_sources.source_corrected, all_sources.source_dt),
            'Unknown'
        ) as _source_corrected,
        coalesce(
            max_by(paid_sources.utm_source, paid_sources.source_dt), 
            max_by(organic_sources.utm_source, organic_sources.source_dt),
            max_by(all_sources.utm_source, all_sources.source_dt),
            'Unknown'
        ) as source,
        coalesce(
            max_by(paid_sources.utm_medium, paid_sources.source_dt), 
            max_by(organic_sources.utm_medium, organic_sources.source_dt),
            max_by(all_sources.utm_medium, all_sources.source_dt),
            'Unknown'
        ) as medium,
        coalesce(
            max_by(paid_sources.device_id, paid_sources.source_dt),
            max_by(organic_sources.device_id, organic_sources.source_dt),
            max_by(all_sources.device_id, all_sources.source_dt)
        ) as source_device_id,       
        coalesce(
            max_by(paid_sources.type, paid_sources.source_dt),
            max_by(organic_sources.type, organic_sources.source_dt),
            max_by(all_sources.type, all_sources.source_dt)
        ) as source_event_type,
        coalesce(
            max_by(paid_sources.campaign_corrected, paid_sources.source_dt), 
            max_by(organic_sources.campaign_corrected, organic_sources.source_dt),
            max_by(all_sources.campaign_corrected, all_sources.source_dt)
        ) as utm_campaign,
        coalesce(
            max_by(paid_sources.campaign_corrected, paid_sources.source_dt), 
            max_by(organic_sources.campaign_corrected, organic_sources.source_dt),
            max_by(all_sources.campaign_corrected, all_sources.source_dt),
            'Unknown'
        ) as _campaign_corrected
    from 
        first_purchases
    full join sources as paid_sources
        on paid_sources.user_email_hash = first_purchases.user_email_hash
        and paid_sources.source_dt <= first_purchases.first_purchase_date_ts_cet
        and lower(paid_sources.source_corrected) not in ('unknown', 'organic', 'unmarked_facebook_or_instagram', 'social', 'email', 'newsletter')
    full join sources as organic_sources
        on organic_sources.user_email_hash = first_purchases.user_email_hash
        and organic_sources.source_dt <= first_purchases.first_purchase_date_ts_cet
        and lower(organic_sources.source_corrected) <> lower('Unknown')
    full join sources as all_sources
        on all_sources.user_email_hash = first_purchases.user_email_hash
        and all_sources.source_dt <= first_purchases.first_purchase_date_ts_cet
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
            when partner = 'onfy' 
            then coalesce(_source_corrected, source)
            else partner
        end) as source_corrected,
        case 
            when partner = 'onfy' 
            then coalesce(_campaign_corrected, utm_campaign)
            else partner
        end as campaign_corrected,
        coalesce(_source_corrected, source) as source_with_partners,
        coalesce(_campaign_corrected, utm_campaign) as campaign_with_partners
    from 
        last_not_unknown_devices_partners
)

SELECT
    t.*,
    coalesce(CAST(source_ts_cet AS DATE), CAST(first_purchase_date_ts_cet AS DATE)) AS partition_date
FROM users_corrected AS t
DISTRIBUTE BY partition_date
