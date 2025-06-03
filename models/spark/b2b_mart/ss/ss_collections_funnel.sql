{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH bots AS (
    SELECT
        device_id,
        MAX(1) AS bot_flag
    FROM threat.bot_devices_joompro
    WHERE
        is_device_marked_as_bot
        OR is_retrospectively_detected_bot
    GROUP BY 1
),

product_preview AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY type, source, event_ts_msk, user_id, promotionId, position, index, productId
            ORDER BY event_id
        ) AS row_n,
        COALESCE(
            LEAD(event_ts_msk) OVER (
                PARTITION BY user_id, source, promotionId, position, index, productId
                ORDER BY event_ts_msk
            ),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_product_preview
    FROM (
        SELECT
            de.type,
            de.payload.source,
            de.event_id,
            de.event_ts_msk,
            de.user.userId AS user_id,
            coalesce(
                de.payload.promotionId,
                nullIf(REGEXP_EXTRACT(de.payload.pageUrl, r'promotions/([^/?]+)'), ''),
                nullIf(REGEXP_EXTRACT(de.payload.pageUrl, r'search/([^/?]+)'), '')
            ) AS promotionId,
            de.payload.position,
            de.payload.index,
            de.payload.productId
        FROM {{ source('b2b_mart', 'device_events') }} AS de
        LEFT JOIN bots ON de.device.id = bots.device_id
        WHERE
            de.partition_date >= '2024-11-07'
            AND de.type = 'productPreview'
            AND de.payload.position IS NOT NULL
            AND bots.bot_flag IS NULL
            AND CASE
                    /* Показ товаров в рамках подборки где-то в каталоге */
                    WHEN de.payload.promotionId IS NOT NULL THEN 1
                    /* Показ товаров в рамках подборки на отдельной странице подборки */
                    WHEN de.payload.source = 'promotion' AND REGEXP_EXTRACT(de.payload.pageUrl, r'promotions/([^/?]+)') != '' THEN 1
                    /* Показ товаров hotSale на отдельной странице */
                    WHEN de.payload.source = 'search' AND REGEXP_EXTRACT(de.payload.pageUrl, r'search/([^/?]+)') LIKE '%hotSale%' THEN 1
                END = 1
    ) AS m
),

/* Клики в подборке */
product_click AS (
    SELECT
        *,
        COALESCE(
            LEAD(event_ts_msk) OVER (
                PARTITION BY user_id, source, promotionId, position, index, productId
                ORDER BY event_ts_msk
            ),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_product_click
    FROM (
        SELECT
            de.payload.source,
            de.event_ts_msk,
            de.user.userId AS user_id,
            coalesce(
                de.payload.promotionId,
                nullIf(REGEXP_EXTRACT(de.payload.pageUrl, r'promotions/([^/?]+)'), ''),
                nullIf(REGEXP_EXTRACT(de.payload.pageUrl, r'search/([^/?]+)'), '')
            ) AS promotionId,
            de.payload.position,
            de.payload.index,
            de.payload.productId
        FROM {{ source('b2b_mart', 'device_events') }} AS de
        WHERE
            de.partition_date >= '2024-11-07'
            AND de.type = 'productClick'
            AND de.payload.position IS NOT NULL
            AND CASE
                    /* Показ товаров в рамках подборки где-то в каталоге */
                    WHEN de.payload.promotionId IS NOT NULL THEN 1
                    /* Показ товаров в рамках подборки на отдельной странице подборки */
                    WHEN de.payload.source = 'promotion' AND REGEXP_EXTRACT(de.payload.pageUrl, r'promotions/([^/?]+)') != '' THEN 1
                    /* Показ товаров hotSale на отдельной странице */
                    WHEN de.payload.source = 'search' AND REGEXP_EXTRACT(de.payload.pageUrl, r'search/([^/?]+)') LIKE '%hotSale%' THEN 1
                END = 1
    ) AS m
),

/* Добавление товара в корзину в течение часа после просмотра товара из подборки */
add_to_cart AS (
    SELECT
        de.user.userId AS user_id,
        de.event_ts_msk,
        de.payload.productId,
        COALESCE(
            LEAD(de.event_ts_msk) OVER (PARTITION BY de.user.userId, de.payload.productId ORDER BY de.event_ts_msk),
            CURRENT_TIMESTAMP() + INTERVAL 3 HOURS
        ) AS next_same_add_to_cart
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    WHERE
        de.partition_date >= '2024-11-07'
        AND de.type = 'addToCart'
),

/* Создание сделки после добавления товара в корзину после просмотра товара из подборки */
create_deal AS (
    SELECT
        d.user_id,
        d.deal_friendly_id,
        d.deal_created_ts AS event_ts_msk,
        REGEXP_EXTRACT(c.link, r'products/([^/?]+)') AS productId
    FROM {{ ref('fact_deals_with_requests') }} AS d
    LEFT JOIN {{ ref('fact_customer_requests') }} AS c
        ON c.next_effective_ts_msk IS NULL
        AND d.deal_id = c.deal_id
),

promotions AS (
    SELECT
        p.promotion_id,
        p.promotion_name,
        SUM(ppg.pids_count) AS promotion_products_count
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
            size(from_json(p.content, 'STRUCT<pids: ARRAY<STRING>>').pids) AS pids_count 
        FROM
            {{ ref('scd2_mongo_promo_product_groups') }} AS p
        WHERE
            p.dbt_valid_to IS NULL
    ) AS ppg ON p.exploded_key = ppg.product_groups_id
    GROUP BY 1, 2
),

main AS (
    SELECT
        main.type,
        main.source,
        main.event_id,
        main.event_ts_msk,
        DATE(main.event_ts_msk) AS event_date_msk,
        main.user_id,
        main.promotionId AS promotion_id,
        main.position,
        main.index,
        main.productId AS product_id,
        /*
        Агрегация здесь для того, чтобы не учитывать несколько кликов в рамках одного показа подборки.
        Иначе может произойти ситуация, что в рамках одного конкретного показа подборки пользователь перешел по одному товару 10 раз.
        */
        MIN(pc.event_ts_msk) AS product_click_at,
        MIN(atc.event_ts_msk) AS add_to_cart_at,
        MIN(mtd.event_ts_msk) AS move_to_deal_at
    FROM (
        SELECT *
        FROM product_preview
        /* Удаляем дубли */
        WHERE row_n = 1
    ) AS main
    LEFT JOIN product_click AS pc
        ON
            main.user_id = pc.user_id
            AND main.source = pc.source
            AND main.promotionId = pc.promotionId
            AND main.position = pc.position
            AND main.index = pc.index
            AND main.productId = pc.productId
            AND pc.event_ts_msk >= main.event_ts_msk
            AND pc.event_ts_msk < main.next_same_product_preview
    LEFT JOIN add_to_cart AS atc
        ON
            pc.user_id = atc.user_id
            AND pc.productId = atc.productId
            AND atc.event_ts_msk >= pc.event_ts_msk
            /* Окно для конвертации клика в добавление в корзину - 24 часа */
            AND atc.event_ts_msk < pc.event_ts_msk + INTERVAL 24 HOURS
            AND atc.event_ts_msk < pc.next_same_product_click
    LEFT JOIN create_deal AS mtd
        ON
            pc.user_id = mtd.user_id
            AND pc.productId = mtd.productId
            AND mtd.event_ts_msk >= pc.event_ts_msk
            AND mtd.event_ts_msk < pc.next_same_product_click
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
)


SELECT
    main.promotion_id,
    CASE
        WHEN lower(main.promotion_id) like '%hotsale%' THEN 'Hot Sale'
        ELSE pi.promotion_name
    END AS promotion_name,
    pi.promotion_products_count,
    main.source,
    main.position,
    main.type AS event_type,
    main.event_id,
    main.event_ts_msk,
    main.event_date_msk,
    main.user_id,
    main.product_id,
    main.index + 1 AS product_index,
    main.product_click_at,
    least(main.add_to_cart_at, coalesce(main.move_to_deal_at, main.add_to_cart_at)) AS add_to_cart_at,
    main.move_to_deal_at
FROM main
LEFT JOIN promotions AS pi ON main.promotion_id = pi.promotion_id
