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
    WHERE is_device_marked_as_bot
       OR is_retrospectively_detected_bot
    GROUP BY device_id
),

events AS (
    SELECT
        DISTINCT user['userId'] AS user_id,
        de.device.id AS device_id,
        de.device.osType AS device_os_type,
        type,
        event_ts_msk
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    LEFT JOIN bots ON de.device.id = bots.device_id
    WHERE partition_date >= '2024-04-01'
      AND bot_flag IS NULL
      AND user['userId'] IS NOT NULL
),

events_with_gap AS (
    SELECT
        user_id,
        device_id,
        device_os_type,
        type,
        event_ts_msk,
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
            WHEN UNIX_TIMESTAMP(event_ts_msk) - UNIX_TIMESTAMP(prev_event_ts_msk) > 60*30 THEN 1
            ELSE 0
        END AS is_new_session_flag
    FROM events_with_gap
),

sessions_with_id AS (
    SELECT
        *,
        sum(is_new_session_flag) OVER (
            PARTITION BY user_id ORDER BY event_ts_msk
        ) AS session_num
    FROM sessions_numbered
),

sessions_with_rownum AS (
    SELECT
        *,
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
        session_num,
        MAX(session_num) OVER (PARTITION BY user_id) AS sessions_count_by_user,
        MIN(event_ts_msk) AS session_start,
        MAX(event_ts_msk) AS session_end,
        UNIX_TIMESTAMP(MAX(event_ts_msk)) - UNIX_TIMESTAMP(MIN(event_ts_msk)) AS session_duration_seconds,
        MAX(CASE WHEN rn_asc = 1 THEN type END) AS first_event_name,
        MAX(CASE WHEN rn_desc = 1 THEN type END) AS last_event_name,
        COUNT(type) AS events_in_session_count,
        COUNT(DISTINCT type) AS events_in_session_unique_count,
        COLLECT_LIST(
            NAMED_STRUCT(
                'event_type', type,
                'event_time', event_ts_msk,
                'device_id', device_id,
                'device_oc_type', device_os_type
            )
        ) AS events_in_session,
        COLLECT_SET(
            NAMED_STRUCT(
                'device_id', device_id,
                'device_oc_type', device_os_type
            )
        ) AS unique_devices_in_session
    FROM sessions_with_rownum
    GROUP BY 1,2
)


SELECT 
    user_id,
    /* Если у юзера за все время жизни было только одно событие deviceCreate, тогда считаем его inactive */
    CASE
        WHEN first_event_name = 'deviceCreate'
         AND events_in_session_count = 1
         AND session_duration_seconds = 0
         AND sessions_count_by_user = 1
        THEN 'inactive'
        ELSE 'active'
    END AS user_type,
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
