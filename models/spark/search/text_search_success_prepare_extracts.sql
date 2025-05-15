{{ config(
    schema='search',
    materialized='table',
    partition_by=['search_date'],
    file_format='parquet',
    meta = {
      'model_owner' : '@itangaev',
      'team': 'search',
      'bigquery_load': 'false'
    }
) }}
WITH searches as (
    SELECT
        device_id,
        payload.query,
        DATE(FROM_UNIXTIME(event_ts / 1000)) AS search_date,
        FIRST(payload.numResults) AS numResults,
        FIRST(event_ts) AS event_ts,
        FIRST(payload.origin.source) AS source,
        FIRST(payload.searchSessionId) AS searchSessionId,
        FIRST(payload.searchRequestId) AS searchRequestId,
        FIRST(device.pref_country) AS device_country,
        FIRST(device.language) AS language,
        FIRST(device.os_type) AS os_type
    FROM {{ source('mart', 'device_events') }}
    WHERE partition_date >= DATE("2025-03-01")
    AND type = "search" AND payload.query IS NOT NULL AND payload.query <> ""
    GROUP BY device_id, payload.query, search_date
),

top_countries as (
    SELECT
        country_code,
        top_country_code,
        region_name
    FROM {{ ref('gold_countries') }}
    WHERE region_name = "Europe"
),

searches_with_top_countries AS (
    SELECT
        s.*,
        t.top_country_code
    FROM searches s
    JOIN top_countries t
    ON (s.device_country == t.country_code)
),

query_stat_by_top_countries as (
    SELECT
        query,
        top_country_code,
        count(DISTINCT device_id, search_date) as freq
    FROM searches_with_top_countries
    GROUP BY 1, 2
),

ranked AS (
    SELECT
        *,
        SUM(freq) OVER (PARTITION BY top_country_code ORDER BY freq DESC ROWS UNBOUNDED PRECEDING) AS cumsum,
        CAST(SUM(freq) OVER (PARTITION BY top_country_code) as Double) AS total
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

        FIRST(lastContext.searchQuery) as clicks_context_query,
        FIRST(lastContext.name) as clicks_context_name,
        FIRST(lastContext.requestId) as request_id,
        FIRST(lastContext.position) as position,

        MAX(IF(type = "productOpen", 1, 0)) AS has_open,
        MAX(IF(type IN ("productToFavorites", "productToCollection", "productToCart"), 1, 0)) AS has_to_cart_or_favorite,
        MAX(IF(type = "productToCart", 1, 0)) AS has_to_cart,
        MAX(IF(type = "productPurchase", 1, 0)) AS has_purchase

    FROM {{ source('mart', 'device_events') }}
    WHERE
        type IN ("productOpen", "productToFavorites", "productToCollection", "productActionClick", "productToCart", "productPurchase")
        AND partition_date >= DATE("2025-03-01")
    GROUP BY device_id, product_id, event_date
),

device_info AS (
    SELECT
        device_id,
        date_msk,
        top_country_code
    FROM {{ ref('gold_active_devices_with_ephemeral') }}
    WHERE
        TRUE
        AND date_msk >= DATE("2025-03-01")  -- начало отсчета метрики
        AND DATEDIFF(date_msk, join_date_msk) <= 30  -- определение нового пользования в данном контексте
)

SELECT
    s.*,
    c.product_id,
    c.position,
    c.has_open,
    c.has_to_cart_or_favorite,
    c.has_to_cart,
    c.has_purchase,
    c.clicks_context_query,
    c.clicks_context_name,
    q.frequency_cluster,
    DATEDIFF(c.event_date, s.search_date) as days_from_search_to_event
FROM searches_with_top_countries s
LEFT JOIN clicks c
ON (s.device_id = c.device_id AND s.search_date <= c.event_date AND s.search_date > c.event_date - INTERVAL 14 DAYS)
JOIN device_info d
ON (s.device_id = d.device_id AND s.search_date = d.date_msk)
JOIN classified_queries_by_frequency q
ON (s.query = q.query and s.top_country_code = q.top_country_code)
