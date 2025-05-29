{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@helbuk',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

---------------------------------------------------------------------

-- RU: Собираем показы, клики, заказы, gmv и кол-во купленных упаковок по баннерам (разные SourceScreen'ы)
-- ENG: Collect shows, clicks, orders, gmv and number of purchased packages produced by banners (different SourceScreens)

---------------------------------------------------------------------

WITH views AS (
    SELECT
        IF(LOWER(device.osType) LIKE '%web%' OR LOWER(device.osType) = 'desktop', 'web', 'app') AS app_device_type,
        payload.sourceScreen,
        partition_date_cet,
        type,
        device_id,
        event_ts_cet,
        event_id,
        IF(payload.pzn IS NOT NULL, payload.pzn, 'banner') AS promo_type,
        payload.blockName
    FROM onfy_mart.device_events
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type IN ('producerBannerShown', 'producerBannerClicked')
),

clicks AS (
    SELECT
        device_id,
        event_ts_cet,
        event_id,
        payload.pzn
    FROM onfy_mart.device_events
    WHERE
        partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND type = 'productOpen'
),

joined_clicks AS (
    SELECT
        views.device_id,
        views.partition_date_cet,
        views.sourceScreen,
        views.promo_type,
        views.blockName,
        views.type,
        views.event_id AS views_event_id,
        clicks.event_ts_cet AS clicks_event_ts_cet,
        clicks.event_id AS clicks_event_id,
        clicks.pzn AS clicks_pzn
    FROM views
    LEFT JOIN clicks
        ON
            views.device_id = clicks.device_id
            AND views.event_ts_cet <= clicks.event_ts_cet
            AND views.event_ts_cet + INTERVAL 30 MINUTE >= clicks.event_ts_cet
),

ranked_orders AS (
    SELECT
        joined_clicks.device_id,
        joined_clicks.partition_date_cet,
        joined_clicks.sourceScreen,
        joined_clicks.promo_type,
        joined_clicks.blockName,
        joined_clicks.clicks_pzn,
        orders.order_id,
        orders.products_price,
        orders.quantity,
        ROW_NUMBER() OVER (PARTITION BY orders.order_id ORDER BY joined_clicks.clicks_event_ts_cet ASC) AS rn
    FROM joined_clicks
    INNER JOIN onfy.orders_info AS orders
        ON
            joined_clicks.device_id = orders.device_id
            AND joined_clicks.clicks_event_ts_cet <= orders.order_created_time_cet
            AND joined_clicks.clicks_event_ts_cet + INTERVAL 5 HOUR >= orders.order_created_time_cet
            AND joined_clicks.clicks_pzn = orders.pzn
    WHERE
        joined_clicks.type = 'producerBannerClicked'
        AND joined_clicks.clicks_event_id IS NOT NULL
),

deduped_orders AS (
    SELECT
        order_id,
        FIRST_VALUE(partition_date_cet) OVER w AS partition_date_cet,
        FIRST_VALUE(sourceScreen) OVER w AS sourceScreen,
        FIRST_VALUE(promo_type) OVER w AS promo_type,
        FIRST_VALUE(blockName) OVER w AS blockName,
        MAX(products_price) OVER w AS products_price,
        MAX(quantity) OVER w AS quantity
    FROM ranked_orders
    WHERE rn = 1
    WINDOW w AS (PARTITION BY order_id)
),

joined_orders AS (
    SELECT
        partition_date_cet,
        sourceScreen,
        promo_type,
        blockName,
        COUNT(*) AS orders,
        SUM(products_price) AS gmv,
        SUM(quantity) AS packs_sold
    FROM deduped_orders
    GROUP BY
        partition_date_cet,
        sourceScreen,
        promo_type,
        blockName
),

groupped_clicks AS (
    SELECT
        partition_date_cet,
        sourceScreen,
        promo_type,
        blockName,
        COUNT(DISTINCT IF(type <> 'producerBannerClicked', views_event_id, NULL)) AS impressions,
        COUNT(DISTINCT IF(type = 'producerBannerClicked', views_event_id, NULL)) AS clicks
    FROM joined_clicks
    GROUP BY
        partition_date_cet,
        sourceScreen,
        promo_type,
        blockName
)

SELECT
    groupped_clicks.partition_date_cet,
    groupped_clicks.sourceScreen,
    groupped_clicks.promo_type,
    groupped_clicks.blockName,
    groupped_clicks.impressions,
    groupped_clicks.clicks,
    COALESCE(joined_orders.orders, 0) AS orders,
    COALESCE(joined_orders.gmv, 0) AS gmv,
    COALESCE(joined_orders.packs_sold, 0) AS packs_sold
FROM groupped_clicks
LEFT JOIN joined_orders
    ON
        groupped_clicks.partition_date_cet = joined_orders.partition_date_cet
        AND groupped_clicks.sourceScreen = joined_orders.sourceScreen
        AND groupped_clicks.promo_type = joined_orders.promo_type
        AND groupped_clicks.blockName = joined_orders.blockName