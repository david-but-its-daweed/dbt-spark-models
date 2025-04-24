{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@a.badoyan',
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

main AS (
    SELECT
        main.type,
        main.event_id,
        main.event_ts_msk,
        DATE(main.event_ts_msk) AS event_date_msk,
        main.user_id,
        main.promotionId AS promotion_id,
        main.position,
        main.productId AS product_id,
        main.index,
        /*
        Агрегация здесь для того, чтобы не учитывать несколько кликов в рамках одного показа подборки.
        Иначе может произойти ситуация, что в рамках одного конкретного показа подборки пользователь перешел по одному товару 10 раз.
        */
        MIN(pc.event_ts_msk) AS product_click_at,
        MIN(atc.event_ts_msk) AS add_to_cart_at,
        MIN(mtd.event_ts_msk) AS move_to_deal_at
    FROM (
        SELECT
            de.type,
            de.event_id,
            de.event_ts_msk,
            de.`user`.userId AS user_id,
            de.payload.promotionId,
            de.payload.position,
            de.payload.productId,
            de.payload.index,
            COALESCE(
                LEAD(de.event_ts_msk) OVER (PARTITION BY de.`user`.userId, de.payload.promotionId, de.payload.position, de.payload.productId ORDER BY de.event_ts_msk),
                CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
            ) AS next_same_product_preview
        FROM {{ source('b2b_mart', 'device_events') }} AS de
        LEFT JOIN bots ON de.device.id = bots.device_id
        WHERE
            de.partition_date >= '2024-11-07'
            AND de.type = 'productPreview'
            AND de.payload.promotionId IS NOT NULL
            AND de.payload.position IS NOT NULL
            AND bots.bot_flag IS NULL
    ) AS main
    /* Присоединиям информацию о кликах в подборке */
    LEFT JOIN ( -- noqa: ST05
        SELECT
            de.`user`.userId AS user_id,
            de.payload.promotionId,
            de.payload.position,
            de.payload.productId,
            de.event_ts_msk,
            COALESCE(
                LEAD(de.event_ts_msk) OVER (PARTITION BY de.`user`.userId, de.payload.promotionId, de.payload.position, de.payload.productId ORDER BY de.event_ts_msk),
                CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
            ) AS next_same_product_click
        FROM {{ source('b2b_mart', 'device_events') }} AS de
        WHERE
            de.partition_date >= '2024-11-07'
            AND de.type = 'productClick'
            AND de.payload.promotionId IS NOT NULL
            AND de.payload.position IS NOT NULL
    ) AS pc
        ON
            main.type = 'productPreview'
            AND pc.event_ts_msk >= main.event_ts_msk
            AND pc.event_ts_msk < main.next_same_product_preview
            AND main.user_id = pc.user_id
            AND main.promotionId = pc.promotionId
            AND main.position = pc.position
            AND main.productId = pc.productId
    /* Присоединиям информацию о добавлении товара в корзину в течение часа после просмотра товара из подборки */
    LEFT JOIN ( -- noqa: ST05
        SELECT
            de.`user`.userId AS user_id,
            de.event_ts_msk,
            de.payload.productId,
            COALESCE(
                LEAD(de.event_ts_msk) OVER (PARTITION BY de.`user`.userId, de.payload.productId ORDER BY de.event_ts_msk),
                CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
            ) AS next_same_add_to_cart
        FROM {{ source('b2b_mart', 'device_events') }} AS de
        WHERE
            de.partition_date >= '2024-11-07'
            AND de.type = 'addToCart'
    ) AS atc
        ON
            atc.event_ts_msk >= pc.event_ts_msk
            AND atc.event_ts_msk < pc.next_same_product_click
            /* Окно для конвертации клика в добавление в корзину - 1 час */
            AND pc.event_ts_msk <= atc.event_ts_msk + INTERVAL 1 HOURS
            AND pc.user_id = atc.user_id
            AND pc.productId = atc.productId
    /* Присоединиям информацию о создании сделки после добавления товара в корзину после просмотра товара из подборки */
    LEFT JOIN ( -- noqa: ST05
        SELECT
            c.user_id,
            c.event_ts_msk,
            c.product_id AS productId
        FROM
            {{ ref('ss_events_cart') }} AS c
        WHERE
            c.event_msk_date >= '2024-11-07'
            AND c.actionType = 'move_to_deal'
    ) AS mtd
        ON
            mtd.event_ts_msk >= atc.event_ts_msk
            AND mtd.event_ts_msk < atc.next_same_add_to_cart
            AND atc.user_id = mtd.user_id
            AND atc.productId = mtd.productId
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

promotions AS (
    SELECT
        p.promotion_id,
        p.promotion_name,
        SUM(ppg.pids_count) AS count_products
    FROM (
        SELECT
            pr.*,
            EXPLODE(pr.pgs) AS (exploded_key, exploded_value)
        FROM
            {{ ref('scd2_mongo_promotions') }} AS pr
        WHERE
            pr.dbt_valid_to IS NULL
    ) AS p
    LEFT JOIN (
        SELECT
            p.*,
            size(from_json(p.content, 'STRUCT<pids: ARRAY<STRING>>').pids) AS pids_count -- noqa: 
        FROM
            {{ ref('scd2_mongo_promo_product_groups') }} AS p
        WHERE
            ppg.dbt_valid_to IS NULL
    ) AS ppg ON p.exploded_key = ppg.product_groups_id
    GROUP BY 1, 2
)

SELECT
    main.type,
    main.event_id,
    main.event_ts_msk,
    main.event_date_msk,
    main.user_id,
    main.promotion_id,
    pi.promotion_name,
    pi.count_products,
    main.position,
    main.product_id,
    main.index,
    main.product_click_at,
    main.add_to_cart_at,
    main.move_to_deal_at
FROM main
LEFT JOIN promotions AS pi ON main.promotion_id = pi.promotion_id