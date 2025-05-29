{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'priority_weight': '150',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_overwrite': 'true',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH sources AS (
    SELECT DISTINCT
        device_id,
        type,
        event_ts_cet AS source_dt,
        CAST(event_ts_cet AS DATE) AS partition_date,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id ORDER BY event_ts_cet) AS next_source_dt,
        COALESCE(
            CASE
                WHEN onfy_mart.device_events.type = 'externalLink'
                    THEN
                        CASE
                            WHEN LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%google%' AND onfy_mart.device_events.payload.params.utm_campaign IS NOT NULL
                                THEN 'google'
                            WHEN LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%billiger%'
                                THEN 'billiger'
                            WHEN onfy_mart.device_events.payload.params.utm_source IS NOT NULL
                                THEN onfy_mart.device_events.payload.params.utm_source
                            WHEN
                                (onfy_mart.device_events.payload.referrer LIKE '%/www.google%')
                                OR (onfy_mart.device_events.payload.referrer LIKE '%/www.bing%')
                                OR (onfy_mart.device_events.payload.referrer LIKE '%/search.yahoo.com%')
                                OR (onfy_mart.device_events.payload.referrer LIKE '%/duckduckgo.com%')
                                OR (LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%google%' AND onfy_mart.device_events.payload.params.utm_campaign IS NULL)
                                THEN 'Organic'
                            WHEN
                                (onfy_mart.device_events.payload.referrer LIKE '%facebook.com%')
                                OR (onfy_mart.device_events.payload.referrer LIKE '%instagram.com%')
                                THEN 'UNMARKED_facebook_or_instagram'
                        END
                WHEN onfy_mart.device_events.type IN ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                    THEN
                        CASE
                            WHEN onfy_mart.device_events.payload.utm_source = 'Unattributed' THEN 'Facebook'
                            WHEN LOWER(onfy_mart.device_events.payload.utm_source) LIKE '%google%' AND LOWER(onfy_mart.device_events.payload.utm_source) NOT LIKE '%organic%' THEN 'Google'
                            WHEN LOWER(onfy_mart.device_events.payload.utm_source) LIKE '%tiktok%' THEN 'TikTok'
                            WHEN onfy_mart.device_events.payload.utm_source = 'Google Organic Search' THEN 'Organic'
                            WHEN payload.fb_install_referrer_campaign_group_name IS NOT NULL THEN 'Facebook'
                            ELSE onfy_mart.device_events.payload.utm_source
                        END
            END,
            'Unknown'
        ) AS utm_source,
        COALESCE(
            CASE
                WHEN onfy_mart.device_events.type = 'externalLink' AND LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%billiger%'
                    THEN 'billiger'
                WHEN onfy_mart.device_events.type = 'externalLink' AND LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%awin%'
                    THEN IF(
                        payload.link LIKE '%yieldkit%', 'http://yieldkit.com/',
                        REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_EXTRACT(payload.link, '[?&]awin_sitename=([^&]+)', 1), '%3A', ':'), '%2F', '/'), '\\+', ' ')
                    )
                WHEN
                    onfy_mart.device_events.type = 'externalLink'
                    AND LOWER(onfy_mart.device_events.payload.params.utm_source) LIKE '%google%'
                    AND LOWER(onfy_mart.device_events.payload.params.utm_campaign) LIKE '%shopping%'
                    THEN 'shopping'
                WHEN onfy_mart.device_events.type = 'externalLink'
                    THEN onfy_mart.device_events.payload.params.utm_campaign
                WHEN onfy_mart.device_events.type IN ('adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
                    THEN
                        CASE
                            WHEN onfy_mart.device_events.payload.utm_source = 'Unattributed' THEN payload.fb_install_referrer_campaign_group_name
                            WHEN
                                LOWER(onfy_mart.device_events.payload.utm_source) LIKE '%google%'
                                AND LOWER(onfy_mart.device_events.payload.utm_source) NOT LIKE '%organic%' THEN onfy_mart.device_events.payload.utm_campaign
                            WHEN LOWER(onfy_mart.device_events.payload.utm_source) LIKE '%tiktok%' THEN 'tiktok'
                            WHEN payload.fb_install_referrer_campaign_group_name IS NOT NULL THEN payload.fb_install_referrer_campaign_group_name
                            ELSE onfy_mart.device_events.payload.utm_campaign
                        END
            END,
            'Unknown'
        ) AS utm_campaign,
        COALESCE(onfy_mart.device_events.payload.params.utm_medium, onfy_mart.device_events.payload.traffic_medium, payload.fb_install_referrer_campaign_name) AS utm_medium,
        CASE
            WHEN LOWER(COALESCE(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) LIKE '%adcha%' THEN 'adchampagne'
            WHEN LOWER(COALESCE(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) LIKE '%rocket%' THEN 'rocket10'
            WHEN LOWER(COALESCE(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) LIKE '%whiteleads%' THEN 'whiteleads'
            WHEN LOWER(COALESCE(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) LIKE '%ohm%' THEN 'ohm'
            WHEN LOWER(COALESCE(onfy_mart.device_events.payload.utm_campaign, onfy_mart.device_events.payload.params.utm_campaign)) LIKE '%mobihunter%' THEN 'mobihunter'
            ELSE 'onfy'
        END AS partner,
        device.ostype AS os_type,
        COALESCE(payload.link, payload.deeplink) AS landing_page
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        1 = 1
        AND type IN ('externalLink', 'adjustInstall', 'adjustReattribution', 'adjustReattributionReinstall', 'adjustReinstall')
        AND COALESCE(payload.referrer, ' ') NOT LIKE '%kairion%'
),


corrected_sources AS (
    SELECT
        sources.*,
        LOWER(COALESCE(utm.source_corrected, sources.utm_source, 'Unknown')) AS source_corrected,
        SPLIT_PART(COALESCE(utm.campaign_corrected, sources.utm_campaign, 'Unknown'), ' ', 1) AS campaign_corrected
    FROM sources
    LEFT JOIN {{ ref("utm_campaigns_corrected") }} AS utm
        ON
            COALESCE(LOWER(utm.utm_campaign), '') = COALESCE(LOWER(sources.utm_campaign), '')
            AND COALESCE(LOWER(utm.utm_source), '') = COALESCE(LOWER(sources.utm_source), '')
)

SELECT *
FROM corrected_sources
DISTRIBUTE BY partition_date
