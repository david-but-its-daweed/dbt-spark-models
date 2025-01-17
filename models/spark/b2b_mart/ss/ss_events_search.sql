
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


WITH search_events AS (
    SELECT user['userId'] AS user_id, 
           event_ts_msk, 
           type, 
           payload.pageUrl, 
           payload.source, 
           payload.productId AS product_id, 
           payload.timeBeforeClick, 
           payload.categories, 
           payload.productsNumber, 
           payload.page AS page_num, 
           payload.query AS query, 
           payload.hotPriceProductsNumber, 
           payload.topProductsNumber, 
           payload.hasNextPage, 
           payload.searchResultsUniqId, 
           payload.page, 
           payload.isSearchByImage AS is_search_by_image,
           payload.index AS index,
           ROW_NUMBER() OVER (
               PARTITION BY payload.searchResultsUniqId, type 
               ORDER BY event_ts_msk
           ) AS event_number
      FROM {{ source('b2b_mart', 'device_events') }}
     WHERE partition_date >= '2024-07-01'
       AND type IN ('productClick', 'search')
       AND payload.searchResultsUniqId IS NOT NULL
),

search AS (
    SELECT *,
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
        SELECT user_id, 
               searchResultsUniqId, 
               event_ts_msk AS search_ts_msk, 
               type, 
               productsNumber AS product_number, 
               query, 
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
    SELECT searchResultsUniqId, 
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


SELECT *
FROM (
    SELECT search.user_id, 
           search.searchResultsUniqId AS search_results_uniq_id, 
           search.search_ts_msk, 
           CAST(search.search_ts_msk AS DATE) AS search_date, 
           search.product_number, 
           search.query, 
           search.is_search_by_image, 
           clicks.click_ts_msk, 
           clicks.position, 
           clicks.product_id, 
           clicks.click_number,
           CASE
               /* Если запрос пустой, то есть событие поиска произошло из каталога, считаем каждое событие уникальным поиском */
               WHEN coalesce(query, '') = '' THEN 1
               /* Проставляем флаг первого поиска в сессии */
               WHEN search_ts_msk = MIN(search_ts_msk) OVER (
                        PARTITION BY user_id, query, session_id
                    ) THEN 1
               ELSE 0
           END AS is_first_search_by_session
    FROM search
    LEFT JOIN clicks ON search.searchResultsUniqId = clicks.searchResultsUniqId
) AS m
/* Оставляем первый поиск в сессии и события с кликами */
WHERE is_first_search_by_session = 1
   OR click_ts_msk IS NOT NULL
