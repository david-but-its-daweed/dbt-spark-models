{{ config(
    schema='onfy',
    materialized='view',
    incremental_strategy='insert_overwrite',
    meta = {
      'team': 'onfy',
      'bigquery_load': 'true'
    }
) }}

WITH numbered_purchases AS (
    SELECT
        *, 
        RANK() OVER (PARTITION BY user_email_hash ORDER BY created ASC) AS purchase_num
    FROM 
        pharmacy_landing.order
),

first_second_purchases AS (
    SELECT DISTINCT 
        first_purchases.user_email_hash,
        from_utc_timestamp(first_purchases.created, 'Europe/Berlin') AS first_purchase_date_ts_cet,
        first_purchases.device_id AS first_purchase_device_id,
        from_utc_timestamp(second_purchases.created, 'Europe/Berlin') AS second_purchase_date_ts_cet,
        second_purchases.device_id AS second_purchase_device_id
    FROM 
        numbered_purchases AS first_purchases
        LEFT JOIN numbered_purchases AS second_purchases
            ON second_purchases.user_email_hash = first_purchases.user_email_hash
            AND second_purchases.purchase_num = 2
    WHERE
        first_purchases.purchase_num = 1
),

sources AS (
    SELECT 
        onfy_mart.device_events.device_id,
        onfy_mart.device_events.type,
        onfy_mart.device_events.payload.link,
        onfy_mart.device_events.event_ts_cet,
        onfy_mart.device_events.payload.referrer,
        COALESCE(
            CASE 
                WHEN onfy_mart.device_events.type = 'externalLink'
                THEN
                    CASE
                        WHEN LOWER(onfy_mart.device_events.payload.params.utm_source) like '%google%' 
                        THEN 'google' 
                        WHEN onfy_mart.device_events.payload.params.utm_source IS NOT NULL
                        THEN onfy_mart.device_events.payload.params.utm_source
                        WHEN 
                            (onfy_mart.device_events.payload.referrer like '%/www.google%') 
                            OR (onfy_mart.device_events.payload.referrer like '%/www.bing%')
                            OR (onfy_mart.device_events.payload.referrer like '%/search.yahoo.com%')
                            OR (onfy_mart.device_events.payload.referrer like '%/duckduckgo.com%')
                        THEN 'Organic'
                        WHEN
                            (onfy_mart.device_events.payload.referrer like '%facebook.com%') 
                            OR (onfy_mart.device_events.payload.referrer like '%instagram.com%')             
                        THEN 'UNMARKED_facebook_or_instagram'      
                    END
                WHEN onfy_mart.device_events.type in ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                THEN
                    CASE
                        WHEN onfy_mart.device_events.payload.utm_source = 'Unattributed' THEN 'Facebook'
                        WHEN onfy_mart.device_events.payload.utm_source IS NULL THEN 'Unknown'
                        WHEN onfy_mart.device_events.payload.utm_source = 'Google Organic Search' THEN 'Organic'
                        ELSE onfy_mart.device_events.payload.utm_source
                    END
            END
        , 'Unknown') AS source,
        CASE 
            WHEN onfy_mart.device_events.type = 'externalLink' 
            THEN onfy_mart.device_events.payload.params.utm_campaign
            else onfy_mart.device_events.payload.utm_campaign 
        END AS utm_campaign
    FROM 
        onfy_mart.device_events
    WHERE 
        onfy_mart.device_events.type IN ('externalLink', 'adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
),

       
last_not_unknown AS (
    SELECT
        first_second_purchases.user_email_hash,
        first_second_purchases.first_purchase_date_ts_cet,
        to_utc_timestamp(first_second_purchases.first_purchase_date_ts_cet, 'Europe/Berlin') AS first_purchase_date_ts_utc,
        first_second_purchases.first_purchase_device_id,
        first_second_purchases.second_purchase_date_ts_cet,
        to_utc_timestamp(first_second_purchases.second_purchase_date_ts_cet, 'Europe/Berlin') AS second_purchase_date_ts_utc,
        first_second_purchases.second_purchase_device_id,
        MAX(not_unknown_sources.event_ts_cet) AS source_ts_cet,
        coalesce(to_utc_timestamp(MAX(not_unknown_sources.event_ts_cet), 'Europe/Berlin'), to_utc_timestamp(max(all_sources.event_ts_cet), 'Europe/Berlin')) AS source_ts_utc,
        COALESCE(MAX_BY(not_unknown_sources.source, not_unknown_sources.event_ts_cet), 'Unknown') AS source,
        COALESCE(
            MAX_BY(not_unknown_sources.device_id, not_unknown_sources.event_ts_cet),
            MAX_BY(all_sources.device_id, all_sources.event_ts_cet)
        ) AS source_device_id,       
        COALESCE(
            MAX_BY(not_unknown_sources.type, not_unknown_sources.event_ts_cet),
            MAX_BY(all_sources.type, all_sources.event_ts_cet)
        ) AS source_event_type,
        MAX_BY(not_unknown_sources.utm_campaign, not_unknown_sources.event_ts_cet) AS utm_campaign
    FROM 
        first_second_purchases
        LEFT JOIN pharmacy_landing.order
            ON first_second_purchases.user_email_hash = pharmacy_landing.order.user_email_hash
        LEFT JOIN sources AS not_unknown_sources
            ON not_unknown_sources.device_id = pharmacy_landing.order.device_id
            AND not_unknown_sources.event_ts_cet < first_second_purchases.first_purchase_date_ts_cet
            AND not_unknown_sources.source <> 'Unknown'
        LEFT JOIN sources AS all_sources
            ON all_sources.device_id = pharmacy_landing.order.device_id
            AND all_sources.event_ts_cet < first_second_purchases.first_purchase_date_ts_cet
    GROUP BY 
        first_second_purchases.user_email_hash,
        first_second_purchases.first_purchase_date_ts_cet,
        first_second_purchases.first_purchase_device_id,
        first_second_purchases.second_purchase_date_ts_cet,
        first_second_purchases.second_purchase_device_id
),

last_not_unknown_devices_partners AS (
    SELECT DISTINCT 
        last_not_unknown.*,
        CASE 
            WHEN LOWER(last_not_unknown.utm_campaign) LIKE '%adcha%' THEN 'adchampagne'
            WHEN LOWER(last_not_unknown.utm_campaign) LIKE '%rocket%' THEN 'rocket10'
            WHEN LOWER(last_not_unknown.utm_campaign) LIKE '%whiteleads%' THEN 'whiteleads'
            WHEN LOWER(last_not_unknown.utm_campaign) LIKE '%ohm%' THEN 'ohm'
            WHEN LOWER(last_not_unknown.utm_campaign) LIKE '%mobihunter%' THEN 'mobihunter'
            ELSE 'onfy'
        END AS partner,
        CASE
            WHEN first_purchase_device.app_type = 'WEB' AND first_purchase_device.device_type = 'DESKTOP' THEN 'web_desktop'
            WHEN first_purchase_device.app_type = 'WEB' AND first_purchase_device.device_type IN ('PHONE', 'TABLET') THEN 'web_mobile'
            WHEN first_purchase_device.app_type = 'ANDROID' THEN 'android'
            WHEN first_purchase_device.app_type = 'IOS' THEN 'ios'
            ELSE first_purchase_device.app_type || '_' || first_purchase_device.device_type
        END AS first_purchase_app_device_type,
        CASE
            WHEN second_purchase_device.app_type = 'WEB' AND second_purchase_device.device_type = 'DESKTOP' THEN 'web_desktop'
            WHEN second_purchase_device.app_type = 'WEB' AND second_purchase_device.device_type IN ('PHONE', 'TABLET') THEN 'web_mobile'
            WHEN second_purchase_device.app_type = 'ANDROID' THEN 'android'
            WHEN second_purchase_device.app_type = 'IOS' THEN 'ios'
            ELSE second_purchase_device.app_type || '_' || second_purchase_device.device_type
        END AS second_purchase_app_device_type,
        CASE
            WHEN source_device.app_type = 'WEB' AND source_device.device_type = 'DESKTOP' THEN 'web_desktop'
            WHEN source_device.app_type = 'WEB' AND source_device.device_type IN ('PHONE', 'TABLET') THEN 'web_mobile'
            WHEN source_device.app_type = 'ANDROID' THEN 'android'
            WHEN source_device.app_type = 'IOS' THEN 'ios'
            ELSE source_device.app_type || '_' || source_device.device_type
        END AS source_app_device_type
    FROM 
        last_not_unknown
        LEFT JOIN pharmacy_landing.device AS first_purchase_device
            ON first_purchase_device.id = last_not_unknown.first_purchase_device_id
        LEFT JOIN pharmacy_landing.device AS second_purchase_device
            ON second_purchase_device.id = last_not_unknown.second_purchase_device_id
        LEFT JOIN pharmacy_landing.device AS source_device
            ON source_device.id = last_not_unknown.source_device_id
),

users_corrected AS (
    SELECT DISTINCT
        last_not_unknown_devices_partners.*,
        LOWER(CASE 
            WHEN last_not_unknown_devices_partners.partner = 'onfy' 
            THEN COALESCE(pharmacy.utm_campaigns_corrected.source_corrected, last_not_unknown_devices_partners.source)
            ELSE last_not_unknown_devices_partners.partner
        END) AS source_corrected,
        CASE 
            WHEN last_not_unknown_devices_partners.partner = 'onfy' 
            THEN COALESCE(pharmacy.utm_campaigns_corrected.campaign_corrected, last_not_unknown_devices_partners.utm_campaign)
            ELSE last_not_unknown_devices_partners.partner
        END AS campaign_corrected
    FROM 
        last_not_unknown_devices_partners
        LEFT JOIN pharmacy.utm_campaigns_corrected
            ON COALESCE(LOWER(pharmacy.utm_campaigns_corrected.utm_campaign), '') = COALESCE(LOWER(last_not_unknown_devices_partners.utm_campaign), '') 
            AND COALESCE(LOWER(pharmacy.utm_campaigns_corrected.utm_source), '') = COALESCE(LOWER(last_not_unknown_devices_partners.source), '') 
),

fb_google_base AS (
    SELECT
        campaign_id,
        campaign_name,
        'facebook' AS source,
        date,
        spend
    FROM 
        pharmacy.fbads_adset_country_insights

        UNION ALL

    SELECT
        campaign_id,
        campaign_name,
        'google' AS source,
        effective_date AS date,
        spend
    FROM 
        pharmacy.google_campaign_insights
),

fb_google_with_partners AS (
    SELECT
        fb_google_base.campaign_id,
        fb_google_base.campaign_name,
        fb_google_base.source,
        CASE 
            WHEN LOWER(fb_google_base.campaign_name) LIKE '%adcha%' THEN 'adchampagne'
            WHEN LOWER(fb_google_base.campaign_name) LIKE '%rocket%' THEN 'rocket10'
            WHEN LOWER(fb_google_base.campaign_name) LIKE '%whiteleads%' THEN 'whiteleads'
            WHEN LOWER(fb_google_base.campaign_name) LIKE '%ohm%' THEN 'ohm'
            WHEN LOWER(fb_google_base.campaign_name) LIKE '%mobihunter%' THEN 'mobihunter'
            ELSE 'onfy'
        END AS partner,
        CASE 
            WHEN exclusions.platform IS NOT NULL THEN exclusions.platform
            WHEN exclusions.platform IS NULL AND LOWER(fb_google_base.campaign_name) LIKE '%ios%' THEN 'ios'
            WHEN exclusions.platform IS NULL AND LOWER(fb_google_base.campaign_name) LIKE '%android%' THEN 'android'
            WHEN exclusions.platform IS NULL AND LOWER(fb_google_base.campaign_name) LIKE '%web%' THEN 'web'
            ELSE 'other'
        END AS campaign_platform,
        fb_google_base.date AS campaign_date_utc,
        fb_google_base.spend
    FROM 
        fb_google_base
        LEFT JOIN pharmacy.ads_campaign_exceptions AS exclusions
            ON fb_google_base.campaign_name = exclusions.campaign_name
), 

united_spends AS (
    SELECT
        campaign_id,
        campaign_name,
        source,
        partner,
        campaign_platform,
        campaign_date_utc AS partition_date,
        spend
    FROM 
        fb_google_with_partners

        UNION ALL

    SELECT
        campaign_id,
        campaign_name,
        CASE
          WHEN source = 'adwords' THEN 'google'
          ELSE source
        END AS source,
        CASE 
            WHEN LOWER(partner) LIKE '%adcha%' THEN 'adchampagne'
            WHEN LOWER(partner) LIKE '%rocket%' THEN 'rocket10'
            WHEN LOWER(partner) LIKE '%whiteleads%' THEN 'whiteleads'
            WHEN LOWER(partner) LIKE '%ohm%' THEN 'ohm'
            WHEN LOWER(partner) LIKE '%mobihunter%' THEN 'mobihunter'
            ELSE partner
        END AS partner,
        CASE 
            WHEN os_type = 'android' THEN os_type
            WHEN os_type = 'ios' THEN os_type
            WHEN os_type = 'web' THEN os_type
            ELSE 'other'
        END AS campaign_platform,
        join_date AS partition_date,
        spend
    FROM 
        pharmacy.partners_insights
),

ads_spends_corrected AS (
    SELECT
        united_spends.partition_date,
        CASE 
            WHEN united_spends.partner = 'onfy' 
            THEN COALESCE (pharmacy.spends_campaigns_corrected.source, united_spends.source, 'Not_from_dict')
            ELSE united_spends.partner
        END AS source_corrected,
        CASE 
            WHEN united_spends.partner = 'onfy' 
            THEN COALESCE (pharmacy.spends_campaigns_corrected.campaign_corrected, united_spends.campaign_name, 'Not_from_dict') 
            ELSE united_spends.partner
        END AS campaign_corrected,
        SUM(spend) AS spend
    FROM 
        united_spends
        LEFT JOIN pharmacy.spends_campaigns_corrected
            ON LOWER(united_spends.campaign_name) = LOWER(pharmacy.spends_campaigns_corrected.campaign_name)
            AND LOWER(united_spends.source) = LOWER(pharmacy.spends_campaigns_corrected.source)
    GROUP BY 
        united_spends.partition_date,
        united_spends.partner,
        source_corrected,
        campaign_corrected,
        united_spends.source,
        united_spends.campaign_name
),

users_spends_campaigns AS (
    SELECT DISTINCT
        users_corrected.user_email_hash,
        users_corrected.first_purchase_date_ts_cet,
        users_corrected.first_purchase_date_ts_utc,
        users_corrected.first_purchase_device_id,
        users_corrected.second_purchase_date_ts_cet,
        users_corrected.second_purchase_date_ts_utc,
        users_corrected.second_purchase_device_id,
        users_corrected.source_ts_cet,
        users_corrected.source_ts_utc,
        users_corrected.source,
        users_corrected.source_device_id,       
        users_corrected.source_event_type,
        users_corrected.utm_campaign,
        users_corrected.partner,
        users_corrected.first_purchase_app_device_type,
        users_corrected.second_purchase_app_device_type,
        users_corrected.source_app_device_type,
        users_corrected.source_corrected,
        users_corrected.campaign_corrected,
        COALESCE(MAX(ads_spends_corrected.partition_date), date_trunc('DAY', users_corrected.first_purchase_date_ts_utc)) AS spend_date,
        COALESCE(date_trunc('DAY', users_corrected.first_purchase_date_ts_utc), MAX(ads_spends_corrected.partition_date)) AS first_purchase_date
    FROM 
        users_corrected
        LEFT JOIN ads_spends_corrected
            ON LOWER(ads_spends_corrected.campaign_corrected) = LOWER(users_corrected.campaign_corrected)
            AND LOWER(ads_spends_corrected.source_corrected) = LOWER(users_corrected.source_corrected)
            AND ads_spends_corrected.partition_date <= date_trunc('DAY', users_corrected.source_ts_utc)
    GROUP BY
        users_corrected.user_email_hash,
        users_corrected.first_purchase_date_ts_cet,
        users_corrected.first_purchase_date_ts_utc,
        users_corrected.first_purchase_device_id,
        users_corrected.second_purchase_date_ts_cet,
        users_corrected.second_purchase_date_ts_utc,
        users_corrected.second_purchase_device_id,
        users_corrected.source_ts_cet,
        users_corrected.source_ts_utc,
        users_corrected.source,
        users_corrected.source_device_id,       
        users_corrected.source_event_type,
        users_corrected.utm_campaign,
        users_corrected.partner,
        users_corrected.first_purchase_app_device_type,
        users_corrected.second_purchase_app_device_type,
        users_corrected.source_app_device_type,
        users_corrected.source_corrected,
        users_corrected.campaign_corrected
),

users_by_day AS (
    SELECT 
        users_spends_campaigns.first_purchase_date AS first_purchase_date,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected,
        count (distinct users_spends_campaigns.user_email_hash) AS first_purchases,
        count(distinct    
            CASE 
                WHEN users_spends_campaigns.second_purchase_date_ts_cet IS NOT NULL 
                THEN users_spends_campaigns.user_email_hash
                ELSE NULL
            END) AS second_purchases
    FROM 
        users_spends_campaigns
    GROUP BY
        users_spends_campaigns.first_purchase_date,
        users_spends_campaigns.source_corrected,
        users_spends_campaigns.campaign_corrected
),


ads_spends_corrected_day AS (
    SELECT 
        date_trunc('DAY', partition_date) AS spend_day,
        source_corrected,
        campaign_corrected,
        SUM(spend) AS spend
    FROM 
        ads_spends_corrected
    GROUP BY 
        date_trunc('DAY', partition_date),
        source_corrected,
        campaign_corrected
)

SELECT DISTINCT
    COALESCE(ads_spends_corrected_day.spend_day, users_by_day.first_purchase_date) AS partition_date, 
    COALESCE(ads_spends_corrected_day.source_corrected, users_by_day.source_corrected) AS source,
    COALESCE(ads_spends_corrected_day.campaign_corrected, users_by_day.campaign_corrected) AS campaign,
    COALESCE(users_by_day.first_purchases, 0) AS first_purchases,
    COALESCE(users_by_day.second_purchases, 0) AS second_purchases, 
    COALESCE(ads_spends_corrected_day.spend, 0) AS spend
FROM 
    ads_spends_corrected_day
    FULL OUTER JOIN users_by_day
        ON ads_spends_corrected_day.spend_day = users_by_day.first_purchase_date
        AND LOWER(ads_spends_corrected_day.campaign_corrected) = LOWER(users_by_day.campaign_corrected)
        AND LOWER(ads_spends_corrected_day.source_corrected) = LOWER(users_by_day.source_corrected)
