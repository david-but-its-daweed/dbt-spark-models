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

WITH bots AS (
    SELECT
        device_id,
        MAX(1) AS bot_flag
    FROM threat.bot_devices_joompro
    WHERE is_device_marked_as_bot OR is_retrospectively_detected_bot
    GROUP BY 1
),

pre_data AS (
    SELECT
        de.`user`['userId'] AS user_id,
        de.device.id AS device_id,
        de.type,
        de.event_ts_msk,
        CAST(de.event_ts_msk AS DATE) AS event_msk_date,
        de.bot_flag,
        de.payload.pageurl,
        de.payload.pagename,
        LEAD(de.type) OVER (PARTITION BY de.`user`['userId'], de.payload.pageurl ORDER BY de.event_ts_msk) AS lead_type,
        LEAD(de.payload.productId) OVER (PARTITION BY de.`user`['userId'], de.payload.pageurl ORDER BY de.event_ts_msk) AS product_id
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    LEFT JOIN bots ON bots.device_id = de.device.id
    WHERE
        de.type IN ('productSelfServiceOpen', 'pageView')
        AND de.payload.pageurl LIKE '%.pro/pt-br/%'
),

views AS (
    SELECT
        device_id,
        user_id,
        bot_flag,
        event_ts_msk,
        event_msk_date,
        COALESCE(product_id, REPLACE(pageurl, 'https://joom.pro/pt-br/products/', '')) AS product_id
    FROM pre_data
    WHERE pagename = 'product'
),

wide_data_add_to_cart AS (
    SELECT
        id AS event_seed,
        de.`user`['userId'] AS user_id,
        type,
        event_ts_msk,
        CAST(event_ts_msk AS DATE) AS event_msk_date,
        payload.pageurl,
        payload.alreadyInCart,
        payload.productId AS product_id,
        EXPLODE(payload.variants) AS variants
    FROM {{ source('b2b_mart', 'device_events') }} AS de
    WHERE
        type IN ('addToCart')
        AND payload.pageurl LIKE '%.pro/pt-br/%'
),

add_to_card AS (
    SELECT
        user_id,
        product_id,
        event_msk_date,
        MIN(event_seed) AS event_seed,
        MIN(event_ts_msk) AS event_ts_msk,
        SUM(variants.quantity) AS qty,
        COUNT(DISTINCT variants.variantId) AS cnt_variants
    FROM
        wide_data_add_to_cart
    GROUP BY
        1, 2, 3
),

final AS (
    SELECT
        v.device_id,
        v.user_id,
        v.bot_flag,
        v.event_ts_msk,
        v.event_msk_date,
        v.product_id,

        atc.event_seed,
        atc.event_ts_msk AS add_to_card_t,
        atc.qty,
        atc.cnt_variants,

        (CASE WHEN atc.qty IS NOT NULL THEN 1 ELSE 0 END) * (ROW_NUMBER() OVER (PARTITION BY atc.event_seed ORDER BY v.event_ts_msk) - 1) AS check_seed
    FROM views AS v
    LEFT JOIN add_to_card AS atc
        ON
            v.user_id = atc.user_id
            AND v.product_id = atc.product_id
            AND v.event_ts_msk < atc.event_ts_msk
)

SELECT
    user_id,
    device_id,
    bot_flag,
    event_ts_msk,
    event_msk_date,
    product_id,
    CASE WHEN check_seed = 0 THEN add_to_card_t END AS add_to_card_t,
    CASE WHEN check_seed = 0 THEN qty END AS qty,
    CASE WHEN check_seed = 0 THEN cnt_variants END AS cnt_variants
FROM final
