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

-- RU: Собираем показы, клики, заказы, gmv и кол-во купленных упаковок по рекомендациям (разные SourceScreen'ы)
-- ENG: Collect shows, clicks, orders, gmv and number of purchased packages produced by recommendations (different SourceScreens)

---------------------------------------------------------------------

WITH views AS (
    SELECT
        partition_date_cet,
        type,
        device_id,
        event_ts_cet,
        event_id,
        IF(LOWER(device.osType) LIKE '%web%' OR LOWER(device.osType) = 'desktop', 'web', 'app') AS app_device_type,
        payload.sourceScreen,
        payload.widgetType,
        payload.pharmacyName,
        payload.pharmacyId,
        payload.isSponsored,
        payload.price,
        payload.pzn,
        payload.productName,
        payload.productId,
        payload.recommendationSlotName,
        payload.recommendationType,
        payload.promoKey
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'productPreview' -- пред показ карточки
        AND payload.widgetType = 'recommendations'
        AND payload.isSponsored
        AND payload.promoKey IS NOT NULL
),

clicks AS (
    SELECT                    -- Open card directly from Recs
        device_id,
        event_ts_cet,
        event_id,
        payload.pzn,
        payload.recommendationSlotName,
        payload.recommendationType,
        payload.sourceScreen,
        payload.widgetType
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'productOpen'
        AND payload.widgetType = 'recommendations'
    UNION ALL
    SELECT                   -- Add to Cart directly from Recs
        device_id,
        event_ts_cet,
        event_id,
        payload.pzn,
        payload.recommendationSlotName,
        payload.recommendationType,
        payload.sourceScreen,
        payload.widgetType
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'addToCart'
        AND payload.widgetType = 'recommendations'
),

joined_clicks AS (
    SELECT
        views.device_id,
        views.partition_date_cet,
        views.sourceScreen,
        views.pzn,
        views.recommendationType,
        views.recommendationSlotName,
        views.promoKey,
        clicks.event_ts_cet AS clicks_event_ts_cet,
        clicks.event_id AS clicks_event_id
    FROM views
    INNER JOIN clicks
        ON
            views.device_id = clicks.device_id
            AND views.event_ts_cet <= clicks.event_ts_cet
            AND views.event_ts_cet + INTERVAL 30 MINUTE >= clicks.event_ts_cet
            AND views.pzn = clicks.pzn
            AND views.sourceScreen = clicks.sourceScreen
            AND views.recommendationType = clicks.recommendationType
            AND views.recommendationSlotName = clicks.recommendationSlotName
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

clicks_info AS (
    SELECT
        partition_date_cet,
        promoKey,
        sourceScreen,
        pzn,
        recommendationType,
        recommendationSlotName,
        COUNT(DISTINCT clicks_event_id) AS clicks
    FROM joined_clicks
    WHERE
        pzn IS NOT NULL
        AND recommendationType IS NOT NULL
        AND recommendationSlotName IS NOT NULL
        AND promoKey IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
),

impressions_info AS (
    SELECT
        partition_date_cet,
        promoKey,
        sourceScreen,
        pzn,
        recommendationType,
        recommendationSlotName,
        COUNT(DISTINCT event_id) AS impressions
    FROM views
    WHERE
        pzn IS NOT NULL
        AND recommendationType IS NOT NULL
        AND recommendationSlotName IS NOT NULL
        AND promoKey IS NOT NULL
    GROUP BY 1, 2, 3, 4, 5, 6
),

orders_info AS (
    SELECT
        partition_date_cet,
        sourceScreen,
        pzn,
        recommendationType,
        recommendationSlotName,
        COUNT(DISTINCT order_id) AS orders,
        SUM(products_price) AS gmv,
        SUM(quantity) AS packs_sold
    FROM (
        SELECT
            joined_clicks.device_id,
            joined_clicks.partition_date_cet,
            joined_clicks.sourceScreen,
            joined_clicks.pzn,
            joined_clicks.recommendationType,
            joined_clicks.recommendationSlotName,
            orders.order_id,
            orders.products_price,
            orders.quantity
        FROM joined_clicks
        INNER JOIN {{ ref('orders_info') }} AS orders
            ON
                joined_clicks.device_id = orders.device_id
                AND joined_clicks.clicks_event_ts_cet <= orders.order_created_time_cet
                AND joined_clicks.clicks_event_ts_cet + INTERVAL 5 HOUR >= orders.order_created_time_cet
                AND joined_clicks.pzn = orders.pzn
        WHERE
            joined_clicks.clicks_event_id IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    GROUP BY 1, 2, 3, 4, 5
)


SELECT
    impressions_info.partition_date_cet,
    impressions_info.promoKey,
    impressions_info.sourceScreen,
    impressions_info.pzn,
    impressions_info.recommendationType,
    impressions_info.recommendationSlotName,
    impressions_info.impressions,
    COALESCE(clicks_info.clicks, 0) AS clicks,
    COALESCE(orders_info.orders, 0) AS orders,
    COALESCE(orders_info.gmv, 0) AS gmv,
    COALESCE(orders_info.packs_sold, 0) AS packs_sold
FROM impressions_info
LEFT JOIN clicks_info
    ON
        impressions_info.partition_date_cet = clicks_info.partition_date_cet
        AND impressions_info.sourceScreen = clicks_info.sourceScreen
        AND impressions_info.pzn = clicks_info.pzn
        AND impressions_info.recommendationType = clicks_info.recommendationType
        AND impressions_info.recommendationSlotName = clicks_info.recommendationSlotName
LEFT JOIN orders_info
    ON
        impressions_info.partition_date_cet = orders_info.partition_date_cet
        AND impressions_info.sourceScreen = orders_info.sourceScreen
        AND impressions_info.pzn = orders_info.pzn
        AND impressions_info.recommendationType = orders_info.recommendationType
        AND impressions_info.recommendationSlotName = orders_info.recommendationSlotName
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11



