{{ config(
    schema='onfy',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

---------------------------------------------------------------------

-- RU: Собираем показы, клики, заказы, gmv и кол-во купленных упаковок по каталогам 
-- ENG: Collect shows, clicks, orders, gmv and number of purchased packages produced by catalogs

---------------------------------------------------------------------

WITH CatalogData AS (
    SELECT
        partition_date_cet,
        type,
        device_id,
        event_ts_cet,
        event_id,
        payload.categoryId,
        payload.categoryName,
        IF(LOWER(device.osType) LIKE '%web%' OR LOWER(device.osType) = 'desktop', 'web', 'app') AS app_device_type
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'catalogOpen'
),

views AS (
    SELECT
        partition_date_cet,
        type,
        device_id,
        event_ts_cet,
        event_id,
        IF(LOWER(device.osType) LIKE '%web%' OR LOWER(device.osType) = 'desktop', 'web', 'app') AS app_device_type,
        payload.sourceScreen,
        payload.pharmacyName,
        payload.pharmacyId,
        payload.isSponsored,
        payload.price,
        payload.pzn,
        payload.productName,
        payload.productId,
        payload.promoKey
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'productPreview'
        AND payload.isSponsored
        AND payload.promoKey IS NOT NULL
        AND payload.sourceScreen = 'catalog'
),

clicks AS (
    SELECT
        device_id,
        event_ts_cet,
        event_id,
        payload.pzn,
        payload.sourceScreen
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'productOpen'
        AND payload.sourceScreen = 'catalog'
    UNION ALL
    SELECT
        device_id,
        event_ts_cet,
        event_id,
        payload.pzn,
        payload.sourceScreen
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'addToCart'
        AND payload.sourceScreen = 'catalog'
),

joined_views AS (
    SELECT
        CatalogData.partition_date_cet,
        CatalogData.device_id,
        CatalogData.event_ts_cet AS catalog_event_ts_cet,
        CatalogData.categoryId,
        CatalogData.categoryName,
        views.pzn,
        views.promoKey,
        views.event_ts_cet AS views_event_ts_cet,
        views.event_id AS views_event_id
    FROM CatalogData
    INNER JOIN views
        ON
            CatalogData.device_id = views.device_id
            AND CatalogData.event_ts_cet <= views.event_ts_cet
            AND CatalogData.event_ts_cet + INTERVAL 2 MINUTE >= views.event_ts_cet
),

joined_clicks AS (
    SELECT
        joined_views.partition_date_cet,
        joined_views.device_id,
        joined_views.catalog_event_ts_cet,
        joined_views.views_event_ts_cet,
        joined_views.categoryId,
        joined_views.categoryName,
        joined_views.pzn,
        joined_views.promoKey,
        clicks.event_ts_cet AS clicks_event_ts_cet,
        clicks.event_id AS clicks_event_id
    FROM joined_views
    INNER JOIN clicks
        ON
            joined_views.device_id = clicks.device_id
            AND joined_views.views_event_ts_cet <= clicks.event_ts_cet
            AND joined_views.views_event_ts_cet + INTERVAL 30 MINUTE >= clicks.event_ts_cet
            AND joined_views.pzn = clicks.pzn
),

clicks_info AS (
    SELECT
        partition_date_cet,
        promoKey,
        pzn,
        categoryId,
        categoryName,
        COUNT(DISTINCT clicks_event_id) AS clicks
    FROM joined_clicks
    WHERE pzn IS NOT NULL AND promoKey IS NOT NULL AND categoryId IS NOT NULL AND categoryName IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

impressions_info AS (
    SELECT
        partition_date_cet,
        promoKey,
        pzn,
        categoryId,
        categoryName,
        COUNT(DISTINCT views_event_id) AS impressions
    FROM joined_views
    WHERE pzn IS NOT NULL AND promoKey IS NOT NULL AND categoryId IS NOT NULL AND categoryName IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5
),

ranked_orders AS (
    SELECT
        joined_clicks.device_id,
        joined_clicks.partition_date_cet,
        joined_clicks.pzn,
        joined_clicks.categoryId,
        joined_clicks.categoryName,
        joined_clicks.promoKey,
        orders.order_id,
        orders.products_price,
        orders.quantity,
        ROW_NUMBER() OVER (PARTITION BY orders.order_id ORDER BY joined_clicks.clicks_event_ts_cet ASC) AS rn
    FROM joined_clicks
    INNER JOIN {{ ref('orders_info') }} AS orders
        ON
            joined_clicks.device_id = orders.device_id
            AND joined_clicks.clicks_event_ts_cet <= orders.order_created_time_cet
            AND joined_clicks.clicks_event_ts_cet + INTERVAL 5 HOUR >= orders.order_created_time_cet
            AND joined_clicks.pzn = orders.pzn
    WHERE joined_clicks.clicks_event_id IS NOT NULL
),

deduped_orders AS (
    SELECT
        partition_date_cet,
        pzn,
        categoryId,
        categoryName,
        promoKey,
        order_id,
        MAX(products_price) AS products_price,
        MAX(quantity) AS quantity
    FROM ranked_orders
    WHERE rn = 1
    GROUP BY partition_date_cet, pzn, categoryId, categoryName, promoKey, order_id
),

orders_info AS (
    SELECT
        partition_date_cet,
        promoKey,
        pzn,
        categoryId,
        categoryName,
        COUNT(DISTINCT order_id) AS orders,
        SUM(products_price) AS gmv,
        SUM(quantity) AS packs_sold
    FROM deduped_orders
    GROUP BY partition_date_cet, promoKey, pzn, categoryId, categoryName
)

SELECT
    impressions_info.partition_date_cet,
    impressions_info.promoKey,
    impressions_info.pzn,
    impressions_info.categoryId,
    impressions_info.categoryName,
    impressions_info.impressions,
    COALESCE(clicks_info.clicks, 0) AS clicks,
    COALESCE(orders_info.orders, 0) AS orders,
    COALESCE(orders_info.gmv, 0) AS gmv,
    COALESCE(orders_info.packs_sold, 0) AS packs_sold
FROM impressions_info
LEFT JOIN clicks_info
    ON
        impressions_info.partition_date_cet = clicks_info.partition_date_cet
        AND impressions_info.pzn = clicks_info.pzn
        AND impressions_info.categoryId = clicks_info.categoryId
        AND impressions_info.categoryName = clicks_info.categoryName
        AND impressions_info.promoKey = clicks_info.promoKey
LEFT JOIN orders_info
    ON
        impressions_info.partition_date_cet = orders_info.partition_date_cet
        AND impressions_info.pzn = orders_info.pzn
        AND impressions_info.categoryId = orders_info.categoryId
        AND impressions_info.categoryName = orders_info.categoryName
        AND impressions_info.promoKey = orders_info.promoKey
