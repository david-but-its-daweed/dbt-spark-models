{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH bots AS (
    SELECT
        device_id,
        MAX(1) AS bot_flag
    FROM threat.bot_devices_joompro
    WHERE
        is_device_marked_as_bot
        OR is_retrospectively_detected_bot
    GROUP BY 1
)

    
SELECT
    de.type,
    de.event_id,
    de.event_ts_msk,
    de.user.userId AS user_id,
    de.payload.bannerId AS banner_id,
    de.payload.source,
    de.payload.position,
    CASE
        WHEN INSTR(de.payload.pageUrl, '?') > 0 THEN SUBSTRING(de.payload.pageUrl, 1, INSTR(de.payload.pageUrl, '?') - 1)
        ELSE de.payload.pageUrl
    END AS page_url,
    de.payload.bannerDestinationUrl AS banner_destination_url,
    COALESCE(
        nullIf(REGEXP_EXTRACT(de.payload.bannerDestinationUrl, r'promotions/([^/?]+)'), ''),
        nullIf(REGEXP_EXTRACT(de.payload.bannerDestinationUrl, r'search/([^/?]+)'), '')
    ) AS promotion_id
FROM {{ source('b2b_mart', 'device_events') }} AS de
LEFT JOIN bots ON de.device.id = bots.device_id
WHERE
    de.partition_date >= '2025-05-22'
    AND de.type = 'bannerView'
    AND de.payload.bannerDestinationUrl IS NOT NULL
    AND bots.bot_flag IS NULL
