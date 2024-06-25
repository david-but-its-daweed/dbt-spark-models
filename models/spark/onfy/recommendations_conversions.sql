{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@easaltykova',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH sessions AS (
    SELECT DISTINCT
        device_id,
        app_device_type,
        is_buyer,
        session_id,
        session_start,
        session_end,
        source,
        campaign
    FROM {{ source('onfy', 'conversion_funnel_new') }}
    WHERE DATE(session_start) >= (CURRENT_DATE() - INTERVAL 3 MONTH)
),

------------------------------------------------------------------------------------------------------
product_preview_reco AS (
    SELECT
        device_id,
        event_ts_cet AS block_shown_ts,
        payload.sourcescreen AS sourcescreen,
        COALESCE(payload.recommendationtype, payload.widgettype) AS reco_block_type,
        payload.pzn AS pzn,
        payload.productposition
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        type = 'productPreview'
        AND (LOWER(payload.widgettype) LIKE '%recommendatio%' OR payload.widgettype = 'previouslyBought')
        AND payload.widgettype IS NOT NULL
        AND payload.sourcescreen IN ('cart', 'home', 'product')
        AND partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
),

------------------------------------------------------------------------------------------------------
-- adding product to cart directly FROM reco block 
add_to_cart_direct AS (
    SELECT
        device_id,
        event_ts_cet AS add_to_cart_ts,
        payload.sourcescreen AS sourcescreen,
        COALESCE(payload.recommendationtype, payload.widgettype) AS reco_block_type,
        payload.pzn AS pzn
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE type = 'addToCart'
    --AND (LOWER(payload.widgetType) LIKE '%recommendatio%' or payload.widgetType = 'previouslyBought')
    --AND payload.widgetType is not null
    AND payload.sourcescreen IN ('cart', 'home', 'product')
    AND partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
),

-- going to product page FROM recommendaiton block
product_open_from_recos AS (
    SELECT
        device_id,
        event_ts_cet AS product_open_from_recos_ts,
        payload.sourcescreen AS sourcescreen,
        COALESCE(payload.recommendationtype, payload.widgettype) AS reco_block_type,
        payload.pzn AS pzn
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE type = 'productOpen'
    --AND (LOWER(payload.widgetType) LIKE '%recommendatio%' or payload.widgetType = 'previouslyBought')
    --AND payload.widgetType is not null
    AND partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
),

-- adding to cart from product page
add_to_cart_from_product AS (
    SELECT
        device_id,
        event_ts_cet AS add_to_cart_from_product_ts,
        payload.pzn AS pzn
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        type = 'addToCart'
        AND payload.widgettype IS NULL
        AND payload.sourcescreen = 'product'
        AND partition_date_cet >= (CURRENT_DATE() - INTERVAL 3 MONTH)
),

-- if user comes FROM reco block to product page AND than add this product to cart
-- we count this add to cart AS it wAS FROM recommendations
add_to_cart_indirect AS (
    SELECT
        product.device_id,
        add_to_cart.add_to_cart_from_product_ts AS add_to_cart_ts,
        product.sourcescreen,
        product.reco_block_type,
        product.pzn
    FROM product_open_from_recos AS product
    INNER JOIN add_to_cart_from_product AS add_to_cart
        ON
            product.device_id = add_to_cart.device_id
            AND product.pzn = add_to_cart.pzn
            AND add_to_cart.add_to_cart_from_product_ts BETWEEN product.product_open_from_recos_ts AND (product.product_open_from_recos_ts + INTERVAL 2 HOUR)
),

add_to_cart AS (
    SELECT
        device_id,
        add_to_cart_ts,
        sourcescreen,
        reco_block_type,
        pzn
    FROM add_to_cart_direct
    UNION ALL
    SELECT
        device_id,
        add_to_cart_ts,
        sourcescreen,
        reco_block_type,
        pzn
    FROM add_to_cart_indirect
),

------------------------------------------------------------------------------------------------------
orders AS (
    SELECT
        device_id,
        (order_created_time_cet + INTERVAL 5 MINUTE) AS order_ts,
        pzn,
        SUM(products_price) AS products_price
    FROM {{ source('onfy', 'orders_info') }}
    WHERE
        DATE(order_created_time_cet) >= (CURRENT_DATE() - INTERVAL 3 MONTH)
        AND pzn IS NOT NULL
    GROUP BY 1, 2, 3
)
------------------------------------------------------------------------------------------------------

SELECT
    sessions.device_id,
    sessions.session_id,
    sessions.app_device_type,
    sessions.is_buyer,
    sessions.session_start,
    sessions.source,
    sessions.campaign,
    product_preview_reco.sourcescreen AS source_screen,
    CASE
        WHEN product_preview_reco.reco_block_type = 'recommendationBestsellers' OR product_preview_reco.reco_block_type LIKE '%bestseller%' THEN 'bestsellers'
        WHEN product_preview_reco.reco_block_type = 'recommendationAlsoBought' OR product_preview_reco.reco_block_type LIKE '%also_bought%' THEN 'also_bought'
        WHEN product_preview_reco.reco_block_type = 'recommendationAlsoViewed' OR product_preview_reco.reco_block_type LIKE '%also_viewed%' THEN 'also_viewed'
        WHEN product_preview_reco.reco_block_type LIKE '%alternatives%' THEN 'alternatives'
        WHEN product_preview_reco.reco_block_type = 'previouslyBought' THEN 'previously_bought'
        WHEN product_preview_reco.reco_block_type = 'recommendationUserViewed' THEN 'user_viewed'
        ELSE product_preview_reco.reco_block_type
    END AS reco_block_type,
    product_preview_reco.pzn,
    MIN(product_preview_reco.block_shown_ts) AS block_shown_ts,
    MIN(product_preview_reco.productposition) AS product_position,
    MIN(add_to_cart.add_to_cart_ts) AS add_to_cart_ts,
    MIN(orders.order_ts) AS order_ts,
    MIN(orders.products_price) AS products_price
FROM sessions
INNER JOIN product_preview_reco
    ON
        sessions.device_id = product_preview_reco.device_id
        AND product_preview_reco.block_shown_ts BETWEEN sessions.session_start AND sessions.session_end
LEFT JOIN add_to_cart
    ON
        product_preview_reco.device_id = add_to_cart.device_id
        AND product_preview_reco.pzn = add_to_cart.pzn
        AND product_preview_reco.sourcescreen = add_to_cart.sourcescreen
        AND add_to_cart_ts BETWEEN block_shown_ts AND (block_shown_ts + INTERVAL 2 HOUR)
LEFT JOIN orders
    ON
        add_to_cart.device_id = orders.device_id
        AND add_to_cart.pzn = orders.pzn
        AND order_ts BETWEEN add_to_cart_ts AND (add_to_cart_ts + INTERVAL 2 HOUR)
WHERE sessions.app_device_type IN ('android', 'ios')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
UNION ALL
SELECT
    sessions.device_id,
    sessions.session_id,
    sessions.app_device_type,
    sessions.is_buyer,
    sessions.session_start,
    sessions.source,
    sessions.campaign,
    product_preview_reco.sourcescreen AS source_screen,
    CASE
        WHEN product_preview_reco.reco_block_type = 'recommendationBestsellers' OR product_preview_reco.reco_block_type LIKE '%bestseller%' THEN 'bestsellers'
        WHEN product_preview_reco.reco_block_type = 'recommendationAlsoBought' OR product_preview_reco.reco_block_type LIKE '%also_bought%' THEN 'also_bought'
        WHEN product_preview_reco.reco_block_type = 'recommendationAlsoViewed' OR product_preview_reco.reco_block_type LIKE '%also_viewed%' THEN 'also_viewed'
        WHEN product_preview_reco.reco_block_type LIKE '%alternatives%' THEN 'alternatives'
        WHEN product_preview_reco.reco_block_type = 'previouslyBought' OR product_preview_reco.reco_block_type LIKE '%orderedBefore%' THEN 'already_bought'
        WHEN product_preview_reco.reco_block_type = 'recommendationUserViewed' THEN 'user_viewed'
        ELSE product_preview_reco.reco_block_type
    END AS reco_block_type,
    product_preview_reco.pzn,
    MIN(product_preview_reco.block_shown_ts) AS block_shown_ts,
    MIN(product_preview_reco.productposition) AS product_position,
    MIN(add_to_cart.add_to_cart_ts) AS add_to_cart_ts,
    MIN(orders.order_ts) AS order_ts,
    MIN(orders.products_price) AS products_price
FROM sessions
INNER JOIN product_preview_reco
    ON
        sessions.device_id = product_preview_reco.device_id
        AND product_preview_reco.block_shown_ts BETWEEN sessions.session_start AND sessions.session_end
LEFT JOIN add_to_cart
    ON
        product_preview_reco.device_id = add_to_cart.device_id
        AND product_preview_reco.pzn = add_to_cart.pzn
        AND product_preview_reco.sourcescreen = add_to_cart.sourcescreen
        AND product_preview_reco.reco_block_type = add_to_cart.reco_block_type
        AND add_to_cart.add_to_cart_ts BETWEEN product_preview_reco.block_shown_ts AND (product_preview_reco.block_shown_ts + INTERVAL 2 HOUR)
LEFT JOIN orders
    ON
        add_to_cart.device_id = orders.device_id
        AND add_to_cart.pzn = orders.pzn
        AND orders.order_ts BETWEEN add_to_cart.add_to_cart_ts AND (add_to_cart.add_to_cart_ts + INTERVAL 2 HOUR)
WHERE sessions.app_device_type NOT IN ('android', 'ios')
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
