{{ config(
    schema = 'search',
    materialized = 'table',
    partition_by = ['search_date'],
    file_format = 'parquet',
    meta = {
        'model_owner': '@itangaev',
        'team': 'search',
        'bigquery_load': 'false'
    }
) }}

WITH searches AS (
    SELECT
        t.*,
        element_at(
            filter(payload.queryFilters['categoryId'].categories, x -> x IS NOT NULL),
            1
        ) AS search_category_id,
        CASE
            WHEN payload.query IS NOT NULL AND payload.query <> '' THEN 'text_search'
            WHEN search_category_id IS NOT NULL THEN 'category_search'
            ELSE 'unknown_search'
        END AS search_type,
        COALESCE(search_category_id, payload.query) AS textQueryOrCategory,
        DATE(FROM_UNIXTIME(event_ts / 1000)) AS search_date
    FROM {{ source('mart', 'device_events') }} AS t
    WHERE partition_date >= DATE('2025-03-01')
        AND type = 'search'
),

search_agg_with_category_names AS (
    SELECT
        s.device_id,
        s.textQueryOrCategory,
        s.search_type,
        s.search_date,
        FIRST(s.payload.query) AS query,
        FIRST(s.search_category_id) AS search_category_id,
        FIRST(s.payload.numResults) AS numResults,
        FIRST(s.event_ts) AS event_ts,
        FIRST(s.payload.origin.source) AS source,
        FIRST(s.payload.searchSessionId) AS searchSessionId,
        FIRST(s.payload.searchRequestId) AS searchRequestId,
        FIRST(s.device.pref_country) AS device_country,
        FIRST(s.device.language) AS language,
        FIRST(s.device.os_type) AS os_type,
        FIRST(a.nameRu) AS category_name
    FROM searches AS s
    LEFT JOIN {{ source('mongo', 'abu_core_catalog_daily_snapshot') }} AS a
        ON s.search_category_id = a._id
    GROUP BY
        s.device_id,
        s.search_type,
        s.textQueryOrCategory,
        s.search_date
),

top_countries AS (
    SELECT
        country_code,
        top_country_code,
        region_name
    FROM {{ ref('gold_countries') }}
    WHERE region_name = 'Europe'
),

searches_with_top_countries AS (
    SELECT
        s.*,
        t.top_country_code
    FROM search_agg_with_category_names AS s
    JOIN top_countries AS t
        ON s.device_country = t.country_code
),

query_stat_by_top_countries AS (
    SELECT
        query,
        top_country_code,
        COUNT(DISTINCT device_id, search_date) AS freq
    FROM searches_with_top_countries
    WHERE search_type = 'text_search'
    GROUP BY query, top_country_code
),

ranked AS (
    SELECT
        *,
        SUM(freq) OVER (
            PARTITION BY top_country_code
            ORDER BY freq DESC
            ROWS UNBOUNDED PRECEDING
        ) AS cumsum,
        CAST(SUM(freq) OVER (PARTITION BY top_country_code) AS DOUBLE) AS total
    FROM query_stat_by_top_countries
),

classified_queries_by_frequency AS (
    SELECT
        query,
        top_country_code,
        freq,
        CASE
            WHEN cumsum <= (total / 3.0) THEN 'head'
            WHEN cumsum <= (2.0 * total / 3.0) THEN 'middle'
            ELSE 'tail'
        END AS frequency_cluster
    FROM ranked
),

clicks AS (
    SELECT
        device_id,
        payload.productId AS product_id,
        DATE(FROM_UNIXTIME(event_ts / 1000)) AS event_date,
        FIRST(lastContext.searchQuery) AS clicks_context_query,
        FIRST(lastContext.name) AS clicks_context_name,
        FIRST(lastContext.requestId) AS request_id,
        FIRST(lastContext.position) AS position,
        MAX(IF(type = 'productOpen', 1, 0)) AS has_open,
        MAX(IF(type IN ('productToFavorites', 'productToCollection', 'productToCart'), 1, 0)) AS has_to_cart_or_favorite,
        MAX(IF(type = 'productToCart', 1, 0)) AS has_to_cart,
        MAX(IF(type = 'productPurchase', 1, 0)) AS has_purchase
    FROM {{ source('mart', 'device_events') }}
    WHERE type IN (
        'productOpen',
        'productToFavorites',
        'productToCollection',
        'productActionClick',
        'productToCart',
        'productPurchase'
    )
        AND partition_date >= DATE('2025-03-01')
    GROUP BY device_id, product_id, event_date
),

product_categories AS (
    SELECT
        id AS product_id,
        TRANSFORM(publicCategoriesExpAbV2, x -> REGEXP_REPLACE(x, '^[0-9]+:', '')) AS clean_category_ids
    FROM {{ source('search', 'actual_index_export') }}
),

device_info AS (
    SELECT
        device_id,
        date_msk,
        top_country_code
    FROM {{ ref('gold_active_devices_with_ephemeral') }}
    WHERE TRUE
        AND date_msk >= DATE('2025-03-01')
        AND DATEDIFF(date_msk, join_date_msk) <= 30
)

SELECT
    s.*,
    c.product_id,
    c.event_date,
    c.position,
    c.has_open,
    c.has_to_cart_or_favorite,
    c.has_to_cart,
    c.has_purchase,
    c.clicks_context_query,
    c.clicks_context_name,
    q.frequency_cluster,
    pc.clean_category_ids,
    IF(ARRAY_CONTAINS(pc.clean_category_ids, s.search_category_id), 1, 0) AS category_relevance,
    DATEDIFF(c.event_date, s.search_date) AS days_from_search_to_event
FROM searches_with_top_countries AS s
LEFT JOIN clicks AS c
    ON s.device_id = c.device_id
    AND s.search_date <= c.event_date
    AND s.search_date > c.event_date - INTERVAL 14 DAYS
JOIN device_info AS d
    ON s.device_id = d.device_id
    AND s.search_date = d.date_msk
LEFT JOIN classified_queries_by_frequency AS q
    ON s.query = q.query
    AND s.top_country_code = q.top_country_code
LEFT JOIN product_categories AS pc
    ON c.product_id = pc.product_id