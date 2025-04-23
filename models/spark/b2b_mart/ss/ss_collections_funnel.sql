{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@abadoyan',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH bots AS (
    SELECT DISTINCT device_id
    FROM threat.bot_devices_joompro
    WHERE is_device_marked_as_bot OR is_retrospectively_detected_bot
),

preview AS (
    SELECT
        type,
        event_id,
        event_ts_msk,
        `user`.userId AS user_id,
        payload.promotionId AS promotion_id,
        payload.position,
        payload.productId AS product_id,
        payload.index,
        COALESCE(
            LEAD(event_ts_msk) OVER (PARTITION BY `user`.userId, payload.promotionId, payload.position, payload.productId ORDER BY event_ts_msk),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_product_preview
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE
        partition_date >= '2024-11-07'
        AND type = 'productPreview'
        AND payload.promotionId IS NOT NULL
        AND payload.position IS NOT NULL
        AND device_id NOT IN (SELECT b.device_id FROM bots AS b)
),

click AS (
    SELECT
        event_ts_msk,
        `user`.userId AS user_id,
        payload.promotionId AS promotion_id,
        payload.position,
        payload.productId AS product_id,
        COALESCE(
            LEAD(event_ts_msk) OVER (PARTITION BY `user`.userId, payload.promotionId, payload.position, payload.productId ORDER BY event_ts_msk),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_product_click
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE
        partition_date >= '2024-11-07'
        AND type = 'productClick'
        AND payload.promotionId IS NOT NULL
        AND payload.position IS NOT NULL
),

cart AS (
    SELECT
        event_ts_msk,
        `user`.userId AS user_id,
        payload.productId AS product_id,
        COALESCE(
            LEAD(event_ts_msk) OVER (PARTITION BY `user`.userId, payload.productId ORDER BY event_ts_msk),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_add_to_cart
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE
        partition_date >= '2024-11-07'
        AND type = 'addToCart'
),

deal AS (
    SELECT
        event_ts_msk,
        user_id,
        product_id
    FROM {{ ref('ss_events_cart') }}
    WHERE
        event_msk_date >= '2024-11-07'
        AND actionType = 'move_to_deal'
),

main AS (
    SELECT
        p.type,
        p.event_id,
        p.event_ts_msk,
        DATE(p.event_ts_msk) AS event_date_msk,
        p.user_id,
        p.promotion_id,
        p.position,
        p.product_id,
        p.index,
        /*
        Агрегация здесь для того, чтобы не учитывать несколько кликов в рамках одного показа подборки.
        Иначе может произойти ситуация, что в рамках одного конкретного показа подборки пользователь перешел по одному товару 10 раз.
        */
        MIN(cl.event_ts_msk) AS product_click_at,
        MIN(ca.event_ts_msk) AS add_to_cart_at,
        MIN(d.event_ts_msk) AS move_to_deal_at
    FROM preview AS p     /* Присоединиям информацию о кликах в подборке */
    LEFT JOIN click AS cl
        ON
            p.event_ts_msk <= cl.event_ts_msk
            AND p.next_same_product_preview > cl.event_ts_msk
            AND p.user_id = cl.user_id
            AND p.promotion_id = cl.promotion_id
            AND p.position = cl.position
            AND p.product_id = cl.product_id
            AND p.type = 'productPreview'
    /* Присоединиям информацию о добавлении товара в корзину в течение часа после просмотра товара из подборки */
    LEFT JOIN cart AS ca
        ON
            cl.event_ts_msk <= ca.event_ts_msk
            AND cl.next_same_product_click > ca.event_ts_msk
            /* Окно для конвертации клика в добавление в корзину - 1 час */
            AND cl.event_ts_msk <= ca.event_ts_msk + INTERVAL 1 HOURS
            AND cl.user_id = ca.user_id
            AND cl.product_id = ca.product_id
    /* Присоединиям информацию о создании сделки после добавления товара в корзину после просмотра товара из подборки */
    LEFT JOIN deal AS d
        ON
            ca.event_ts_msk <= d.event_ts_msk
            AND ca.next_same_add_to_cart > d.event_ts_msk
            AND ca.user_id = d.user_id
            AND ca.product_id = d.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

promotions AS (
    SELECT
        *,
        EXPLODE(pgs) AS (exploded_key, exploded_value)
    FROM {{ ref('scd2_mongo_promotions') }}
    WHERE dbt_valid_to IS NULL
),

product_groups AS (
    SELECT
        *,
        SIZE(FROM_JSON(content, 'STRUCT<pids: ARRAY<STRING>>').pids) AS pids_count  -- noqa: PRS

    FROM {{ ref('scd2_mongo_promo_product_groups') }}
    WHERE dbt_valid_to IS NULL
),

promotions_full AS (
    SELECT
        p.promotion_id,
        p.promotion_name,
        SUM(ppg.pids_count) AS count_products
    FROM promotions AS p
    LEFT JOIN product_groups AS ppg ON p.exploded_key = ppg.product_groups_id
    GROUP BY 1, 2
)

SELECT
    main.type,

    main.event_id,
    main.event_ts_msk,
    main.event_date_msk,

    main.user_id,

    main.promotion_id,
    pf.promotion_name,

    pf.count_products,
    main.position,

    main.product_id,
    main.index,
    main.product_click_at,
    main.add_to_cart_at,
    main.move_to_deal_at
FROM main
LEFT JOIN promotions_full AS pf ON main.promotion_id = pf.promotion_id