{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
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

fake_search AS (
    SELECT
        s.user_id,
        COUNT(DISTINCT e.event_type) = 1 AND MAX(e.event_type) = 'search' AS is_fake
    FROM
        {{ source('b2b_mart', 'ss_events_by_session') }} AS s
        LATERAL VIEW EXPLODE(events_in_session) AS e
    WHERE
        DATE(s.session_start) >= '2024-07-01'
    GROUP BY 1
    HAVING is_fake = TRUE
),

real_search AS (
    SELECT DISTINCT FROM_JSON(e.event_params, 'searchResultsUniqId STRING').searchResultsUniqId AS search_id
    FROM
        {{ source('b2b_mart', 'ss_events_by_session') }}
        LATERAL VIEW EXPLODE(events_in_session) AS e
    WHERE e.event_type != 'search'
),

search_events AS (
    SELECT
        de.`user`['userId'] AS user_id,
        de.event_ts_msk,
        de.type,
        device.osType AS device_os_type,
        payload.pageUrl,
        payload.source,
        payload.productId AS product_id,
        payload.timeBeforeClick,
        payload.categories,
        payload.productsNumber,
        payload.page AS page_num,
        payload.query,
        payload.hotPriceProductsNumber,
        payload.topProductsNumber,
        payload.hasNextPage,
        payload.searchResultsUniqId,
        payload.page,
        payload.isSearchByImage AS is_search_by_image,
        payload.index,
        ROW_NUMBER() OVER (
            PARTITION BY payload.searchResultsUniqId, de.type
            ORDER BY de.event_ts_msk
        ) AS event_number
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    LEFT JOIN bots AS b ON de.device.id = b.device_id
    LEFT JOIN fake_search AS fs ON de.`user`['userId'] = fs.user_id
    WHERE
        de.partition_date >= '2024-07-01'
        AND de.type IN ('productClick', 'search')
        AND payload.searchResultsUniqId IS NOT NULL
        AND b.bot_flag IS NULL
        AND fs.user_id IS NULL
),

search AS (
    SELECT
        *,
        /* Определяем номер сессии поиска клиента в рамках одного запроса */
        SUM(
            CASE
                WHEN prev_same_search_ts_msk IS NULL THEN 1
                WHEN search_ts_msk >= prev_same_search_ts_msk + INTERVAL 15 MINUTE THEN 1
                ELSE 0
            END
        ) OVER (
            PARTITION BY user_id, query
            ORDER BY search_ts_msk
        ) AS session_id
    FROM (
        SELECT
            user_id,
            searchResultsUniqId,
            event_ts_msk AS search_ts_msk,
            type,
            productsNumber AS product_number,
            query,
            device_os_type,
            is_search_by_image,
            LAG(event_ts_msk) OVER (
                PARTITION BY user_id, query
                ORDER BY event_ts_msk
            ) AS prev_same_search_ts_msk
        FROM search_events
        WHERE type = 'search'
    ) AS s
),

clicks AS (
    SELECT
        searchResultsUniqId,
        event_ts_msk AS click_ts_msk,
        index AS position,
        product_id,
        ROW_NUMBER() OVER (
            PARTITION BY searchResultsUniqId
            ORDER BY event_ts_msk
        ) AS click_number
    FROM search_events
    WHERE type = 'productClick'
)

SELECT
    m.*,

    FIRST_VALUE(m.search_results_uniq_id) OVER (
        PARTITION BY m.user_id, m.search_date, m.query, m.search_type
        ORDER BY m.search_ts_msk
    ) AS search_group_id,

    rs.search_id IS NOT NULL AS was_real_search
FROM (
    SELECT
        search.user_id,
        search.searchResultsUniqId AS search_results_uniq_id,
        search.search_ts_msk,
        CAST(search.search_ts_msk AS DATE) AS search_date,
        search.product_number,
        search.query,
        search.is_search_by_image,
        CASE
            WHEN search.query = '' THEN 'catalog'
            WHEN search.query > '' AND search.is_search_by_image = TRUE THEN 'image'
            WHEN search.query > '' AND search.is_search_by_image IS NULL THEN 'query'
        END AS search_type,

        CASE
            WHEN search.device_os_type IN ('android', 'ios', 'tizen', 'harmonyos') THEN 'mobile'
            WHEN search.device_os_type IN ('ubuntu', 'linux', 'mac os', 'windows', 'chromium os') THEN 'desktop'
            ELSE 'other'
        END AS device_platform,

        clicks.click_ts_msk,
        clicks.position,
        clicks.product_id,
        clicks.click_number,
        CASE
            /* Если запрос пустой, то есть событие поиска произошло из каталога, считаем каждое событие уникальным поиском */
            WHEN COALESCE(search.query, '') = '' THEN 1
            /* Проставляем флаг первого поиска в сессии */
            WHEN search.search_ts_msk = MIN(search.search_ts_msk) OVER (
                PARTITION BY search.user_id, search.query, search.session_id
            ) THEN 1
            ELSE 0
        END AS is_first_search_by_session
    FROM search
    LEFT JOIN clicks ON search.searchResultsUniqId = clicks.searchResultsUniqId
) AS m
LEFT JOIN real_search AS rs ON m.search_results_uniq_id = rs.search_id
/* Оставляем первый поиск в сессии и события с кликами */
WHERE m.is_first_search_by_session = 1 OR m.click_ts_msk IS NOT NULL