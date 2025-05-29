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
WHERE
    de.partition_date >= '2025-05-22'
    AND de.type = 'bannerView'
    AND de.payload.bannerDestinationUrl IS NOT NULL
