{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@daweed',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH bots AS (
    SELECT
        device_id,
        MAX(1) AS bot_flag
    FROM
        threat.bot_devices_joompro
    WHERE
        is_device_marked_as_bot OR is_retrospectively_detected_bot
    GROUP BY
        1
),

events AS (
    SELECT DISTINCT
        DATE(de.event_ts_msk) AS partition_date,
        de.event_ts_msk AS event_ts,
        `user`.userId AS user_id,
        de.type AS event_type,

        CAST(payload.source AS STRING) AS source,
        CASE
            WHEN de.type = 'search' AND CAST(payload.query AS STRING) = '' THEN 'category'
            WHEN de.type = 'search' AND CAST(payload.query AS STRING) > '' AND CAST(payload.isSearchByImage AS BOOLEAN) IS NULL THEN 'query'
            WHEN de.type = 'search' AND CAST(payload.query AS STRING) > '' AND CAST(payload.isSearchByImage AS BOOLEAN) = TRUE THEN 'image'
        END AS search_type,
        CAST(payload.searchResultsUniqId AS STRING) AS search_id,
        CAST(payload.productId AS STRING) AS product_id,
        CAST(payload.index AS INT) AS index
    FROM b2b_mart.device_events AS de
    LEFT JOIN bots AS b ON de.device.id = b.device_id
    WHERE
        de.partition_date >= '2024-01-01'
        AND de.`user`.userId IS NOT NULL
        AND de.type IN ('search', 'productPreview', 'productClick', 'addToCart')
        AND b.bot_flag IS NULL
),

events_with_step AS (
    SELECT
        partition_date,
        event_ts,
        user_id,
        event_type,

        source,
        MAX(search_type) OVER (PARTITION BY search_id) AS search_type,
        search_id,
        product_id,
        index,
        CASE
            WHEN event_type = 'search' THEN 1
            WHEN event_type = 'productPreview' THEN 2
            WHEN event_type = 'productClick' THEN 3
            WHEN event_type = 'addToCart' THEN 4
        END AS funnel_step
    FROM events
),

funnel_progress AS (
    SELECT
        user_id,
        search_id,
        product_id,
        MAX(COALESCE(funnel_step, 0)) AS max_step
    FROM events_with_step
    WHERE search_id IS NOT NULL AND product_id IS NOT NULL
    GROUP BY user_id, search_id, product_id
)

SELECT
    e.*,
    fp.max_step AS funnel_progress_step
FROM events_with_step AS e
LEFT JOIN funnel_progress AS fp
    ON
        e.user_id = fp.user_id
        AND e.search_id = fp.search_id
        AND e.product_id = fp.product_id
ORDER BY
    e.user_id,
    e.event_ts,
    e.index,
    e.funnel_step