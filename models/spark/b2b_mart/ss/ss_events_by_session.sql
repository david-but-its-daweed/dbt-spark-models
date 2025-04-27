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
    FROM
        threat.bot_devices_joompro
    WHERE
        is_device_marked_as_bot OR is_retrospectively_detected_bot
    GROUP BY
        1
),

events AS (
    SELECT DISTINCT
        `user`.userId AS user_id,
        device.id AS device_id,
        device.osType AS device_os_type,
        de.type,
        de.event_ts_msk,
        TO_JSON(
            MAP_FILTER(
                MAP(
                    'pageUrl', CAST(payload.pageUrl AS STRING),
                    'page', CAST(payload.page AS STRING),
                    'pageName', CAST(payload.pageName AS STRING),
                    'source', CAST(payload.source AS STRING),
                    'product_id', CAST(payload.productId AS STRING),
                    'timeBeforeClick', CAST(payload.timeBeforeClick AS STRING),
                    'productsNumber', CAST(payload.productsNumber AS STRING),
                    'query', CAST(payload.query AS STRING),
                    'topProductsNumber', CAST(payload.topProductsNumber AS STRING),
                    'hasNextPage', CAST(payload.hasNextPage AS STRING),
                    'searchResultsUniqId', CAST(payload.searchResultsUniqId AS STRING),
                    'isSearchByImage', CAST(payload.isSearchByImage AS STRING),
                    'index', CAST(payload.index AS STRING),
                    'position', CAST(payload.position AS STRING),
                    'promotionId', CAST(payload.promotionId AS STRING)
                ),
                (k, v) -> v IS NOT NULL
            )
        ) AS event_params
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    LEFT JOIN bots AS b ON de.device.id = b.device_id
    WHERE
        de.partition_date >= '2024-04-01'
        AND de.`user`.userId IS NOT NULL
        AND de.type NOT IN ('deviceCreate')
        AND bots.bot_flag IS NULL
),

events_with_gap AS (
    SELECT
        user_id,
        device_id,
        device_os_type,
        CASE
            WHEN device_os_type IN ('android', 'ios', 'tizen', 'harmonyos') THEN 'mobile'
            WHEN device_os_type IN ('ubuntu', 'linux', 'mac os', 'windows', 'chromium os') THEN 'desktop'
            ELSE 'other'
        END AS device_platform,
        type,
        event_ts_msk,
        event_params,
        LAG(event_ts_msk) OVER (
            PARTITION BY user_id ORDER BY event_ts_msk
        ) AS prev_event_ts_msk
    FROM events
),

sessions_numbered AS (
    SELECT
        *,
        /* Если больше 30 мин - новая сессия */
        CASE
            WHEN prev_event_ts_msk IS NULL THEN 1
            WHEN UNIX_TIMESTAMP(event_ts_msk) - UNIX_TIMESTAMP(prev_event_ts_msk) > 60 * 30 THEN 1
            ELSE 0
        END AS is_new_session_flag
    FROM events_with_gap
),

sessions_with_id AS (
    SELECT
        *,
        SUM(is_new_session_flag) OVER (
            PARTITION BY user_id ORDER BY event_ts_msk
        ) AS session_num
    FROM sessions_numbered
),

sessions_with_rownum AS (
    SELECT
        *,
        CONCAT(user_id, 's', session_num) AS session_id,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, session_num
            ORDER BY event_ts_msk ASC
        ) AS rn_asc,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, session_num
            ORDER BY event_ts_msk DESC
        ) AS rn_desc
    FROM sessions_with_id
),

final AS (
    SELECT
        user_id,
        session_id,
        session_num,
        TO_TIMESTAMP(MIN(event_ts_msk)) AS session_start,
        TO_TIMESTAMP(MAX(event_ts_msk)) AS session_end,
        UNIX_TIMESTAMP(MAX(event_ts_msk)) - UNIX_TIMESTAMP(MIN(event_ts_msk)) AS session_duration_seconds,
        MAX(CASE WHEN rn_asc = 1 THEN type END) AS first_event_name,
        MAX(CASE WHEN rn_desc = 1 THEN type END) AS last_event_name,
        COUNT(type) AS events_in_session_count,
        COUNT(DISTINCT type) AS events_in_session_unique_count,
        COLLECT_LIST(
            NAMED_STRUCT(
                'event_type', type,
                'event_params', event_params,
                'event_time', event_ts_msk,
                'device_id', device_id,
                'device_oc_type', device_os_type,
                'device_platform', device_platform
            )
        ) AS events_in_session,
        COLLECT_SET(
            NAMED_STRUCT(
                'device_id', device_id,
                'device_oc_type', device_os_type,
                'device_platform', device_platform
            )
        ) AS unique_devices_in_session
    FROM sessions_with_rownum
    GROUP BY 1, 2, 3
)

SELECT
    user_id,
    session_id,
    session_num,
    session_start,
    session_end,
    session_duration_seconds,
    first_event_name,
    last_event_name,
    events_in_session_count,
    events_in_session_unique_count,
    events_in_session,
    unique_devices_in_session
FROM final