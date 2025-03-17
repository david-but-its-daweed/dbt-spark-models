{{ config(
    schema='fluff',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@product-analytics.duty',
      'team': 'product',
      'bigquery_load': 'true'
    }
) }}

WITH description_and_merchant_name AS (
    SELECT
        pp.product_id,
        pp.merchant_name,
        pp.orig_description
    FROM {{ source('mart', 'dim_published_product_with_merchant') }} AS pp
    INNER JOIN {{ ref('fluff_products') }} USING (product_id)
    WHERE TO_DATE(pp.next_effective_ts) > '3000-01-01'
),

orders AS (
    SELECT
        so.product_id,
        SUM(IF(so.app_entity = 'fluff', 1, 0)) AS fluff_orders_count,
        ROUND(AVG(IF(so.app_entity = 'fluff' AND so.refund_reason IN ('other', 'fraud', 'quality', 'not_delivered'), 1, CAST(NULL AS INT))) * 100, 1) AS fluff_refund_rate,
        ROUND(AVG(IF(so.app_entity = 'fluff' AND so.refund_reason IN ('cancelled_by_customer', 'cancelled_by_merchant'), 1, CAST(NULL AS INT))) * 100, 1) AS fluff_canceled_rate,
        SUM(IF(so.app_entity = 'joom' AND so.partition_date >= '2025-03-03', 1, 0)) AS joom_orders_count_since_03_03,
        ROUND(
            AVG(
                IF(
                    so.app_entity = 'joom'
                    AND so.partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
                    AND so.refund_reason IN ('other', 'fraud', 'quality', 'not_delivered'),
                    1,
                    CAST(NULL AS INT)
                )
            ) * 100, 1
        ) AS joom_refund_rate_last_3_month,
        ROUND(
            AVG(
                IF(
                    so.app_entity = 'joom'
                    AND so.partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
                    AND so.refund_reason IN ('cancelled_by_customer', 'cancelled_by_merchant'),
                    1,
                    CAST(NULL AS INT)
                )
            ) * 100, 1
        ) AS joom_canceled_rate_last_3_month
    FROM {{ source('mart', 'star_order_2020') }} AS so
    INNER JOIN {{ ref('fluff_products') }} USING (product_id)
    WHERE
        so.partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
        OR so.partition_date >= '2025-03-03'
    GROUP BY so.product_id
),

fbj_stock AS (
    SELECT
        fb.product_id,
        SUM(fb.number_of_products_in_stock) AS fbj_number_of_products_in_stock,
        COLLECT_LIST(NAMED_STRUCT('product_variant_id', fb.product_variant_id, 'number_of_products_in_stock', fb.number_of_products_in_stock)) AS fbj_number_of_products_in_stock_by_variant_id
    FROM {{ ref('fbj_product_stocks_current') }} AS fb
    INNER JOIN {{ ref('fluff_products') }} USING (product_id)
    GROUP BY fb.product_id
),

eu_countries AS (
    SELECT EXPLODE(country_codes) AS country
    FROM {{ source('mart', 'dim_region') }}
    WHERE
        next_effective_ts > '3000-01-01'
        AND region = 'EU'
),

eu_available_products_with_price AS (
    SELECT
        pp.product_id,
        AVG(pp.merchant_price_usd) AS merchant_price_usd
    FROM {{ source('price', 'preview_prices') }} AS pp
    --  INNER JOIN {{ ref('fluff_products') }} USING (product_id) нужны все цены для следующей таблицы дублей
    INNER JOIN eu_countries AS ec ON pp.country = ec.country
    WHERE
        pp.partition_date > CURRENT_DATE() - INTERVAL '92' DAY
    GROUP BY 1
),

fluff_products_double_group_id AS (
    SELECT
        pg.groupId AS product_double_group_id,
        pg.productId AS product_id,
        MAX(IF(fp.product_id IS NOT NULL, 1, 0)) OVER (PARTITION BY pg.groupId) AS is_fluff_double_group_id,
        ROW_NUMBER() OVER (PARTITION BY pg.groupId ORDER BY ea.merchant_price_usd) AS duble_number_by_price
    FROM {{ source('default', 'productsGroups_louvain') }} AS pg
    LEFT JOIN {{ ref('fluff_products') }} AS fp ON fp.product_id = pg.productId
    LEFT JOIN eu_available_products_with_price AS ea ON ea.product_id = pg.productId
),

doubles_list AS (
    SELECT
        product_double_group_id,
        COLLECT_LIST(product_id) AS double_products_list
    FROM
        fluff_products_double_group_id
    WHERE
        is_fluff_double_group_id = 1
        AND duble_number_by_price <= 10
    GROUP BY 1
),

pi_raw AS (
    SELECT
        pi.joom_product_id AS product_id,
        pi.partition_date,
        pi.full_price_with_vat_joom_to_aliexpress_rate,
        pi.special_price_with_vat_joom_to_aliexpress_rate
    FROM {{ source('mi_analytics', 'aliexpress_joom_automatching_price_index') }} AS pi
    INNER JOIN {{ ref('fluff_products') }} AS fp ON fp.product_id = pi.joom_product_id
    WHERE pi.partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
),

pi AS (
    SELECT
        pi.product_id,
        ROUND(PERCENTILE_APPROX(pi.full_price_with_vat_joom_to_aliexpress_rate, 0.5), 1) AS price_index,
        ROUND(PERCENTILE_APPROX(pi.special_price_with_vat_joom_to_aliexpress_rate, 0.5), 1) AS sp_price_index
    FROM pi_raw AS pi
    INNER JOIN {{ ref('fluff_products') }} USING (product_id)
    GROUP BY pi.product_id
),

order_app_entity_fluff_products AS (
    SELECT
        so.product_id,
        so.order_id,
        so.app_entity
    FROM {{ source('mart', 'star_order_2020') }} AS so
    INNER JOIN {{ ref('fluff_products') }} USING (product_id)
    WHERE so.partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
),

logistics_order AS (
    SELECT
        lo.product_id,
        PERCENTILE_APPROX(IF(oa.app_entity = 'fluff', lo.fulfilling_days, NULL), 0.8) AS fluff_fulfilling_days_last_3_month_80th_perc,
        PERCENTILE_APPROX(IF(oa.app_entity = 'joom', lo.fulfilling_days, NULL), 0.8) AS joom_fulfilling_days_last_3_month_80th_perc
    FROM (
        SELECT
            fo.order_id,
            fo.product_id,
            DATEDIFF(fo.check_in_time_utc, fo.order_created_date_utc) AS fulfilling_days
        FROM {{ source('logistics_mart', 'fact_order') }} AS fo
        WHERE
            fo.order_created_date_msk >= CURRENT_DATE() - INTERVAL '92' DAY
            AND fo.refund_type NOT IN ('cancelled_by_customer', 'cancelled_by_merchant')
            AND fo.check_in_time_utc IS NOT NULL
    ) AS lo
    INNER JOIN order_app_entity_fluff_products AS oa USING (order_id)
    GROUP BY lo.product_id
),

conversion_raw AS (
    SELECT
        de.device_id,
        de.payload.productId AS product_id,
        MAX(de.device.pref_country) AS pref_country,
        MAX(de.device.os_type) AS os_type,
        MAX(IF(de.type = 'productPreview', 1, 0)) AS productPreview,
        MAX(IF(de.type = 'productOpenServer', 1, 0)) AS productOpenServer,
        MAX(IF(de.type = 'productToCart', 1, 0)) AS productToCart,
        MAX(IF(de.type = 'productPurchase', 1, 0)) AS productPurchase
    FROM {{ source('mart', 'device_events') }} AS de
    INNER JOIN {{ ref('fluff_products') }} AS fp ON fp.product_id = de.payload.productId
    WHERE
        de.partition_date >= '2025-03-03'
        AND de.device.app_entity = 'fluff'
        AND de.ephemeral = FALSE
        AND de.type IN ('productPreview', 'productOpenServer', 'productToCart', 'productPurchase')
    GROUP BY 1, 2
),

conversion AS (
    SELECT
        product_id,
        SUM(productPreview) AS preview_count,
        ROUND(AVG(IF(productPreview = 1, productOpenServer, NULL)) * 100, 1) AS preview_to_open_perc,
        SUM(productOpenServer) AS open_count,
        ROUND(AVG(IF(productOpenServer = 1, productToCart, NULL)) * 100, 1) AS open_to_cart_perc,
        SUM(productToCart) AS to_cart_count,
        ROUND(AVG(IF(productToCart = 1, productPurchase, NULL)) * 100, 1) AS cart_to_purchase_perc,
        SUM(productPurchase) AS purchase_count
    FROM conversion_raw
    GROUP BY product_id
)

SELECT
    p.product_id,
    p.merchant_id,
    p.product_name,
    p.is_available,
    p.labels,
    p.kam_name,
    dm.merchant_name,
    dm.orig_description,
    o.fluff_orders_count,
    o.fluff_refund_rate,
    o.fluff_canceled_rate,
    o.joom_orders_count_since_03_03,
    o.joom_refund_rate_last_3_month,
    o.joom_canceled_rate_last_3_month,
    fs.fbj_number_of_products_in_stock,
    fs.fbj_number_of_products_in_stock_by_variant_id,
    dg.product_double_group_id,
    dl.double_products_list,
    pi.price_index,
    pi.sp_price_index,
    lo.fluff_fulfilling_days_last_3_month_80th_perc,
    lo.joom_fulfilling_days_last_3_month_80th_perc,
    IF(eu.product_id IS NOT NULL, TRUE, FALSE) AS is_available_in_eu,
    c.preview_count,
    c.preview_to_open_perc,
    c.open_count,
    c.open_to_cart_perc,
    c.to_cart_count,
    c.cart_to_purchase_perc,
    c.purchase_count
FROM {{ ref('fluff_products') }} AS p
LEFT JOIN description_and_merchant_name AS dm ON p.product_id = dm.product_id
LEFT JOIN orders AS o ON p.product_id = o.product_id
LEFT JOIN fbj_stock AS fs ON p.product_id = fs.product_id
LEFT JOIN pi ON p.product_id = pi.product_id
LEFT JOIN logistics_order AS lo ON p.product_id = lo.product_id
LEFT JOIN eu_available_products_with_price AS eu ON p.product_id = eu.product_id
LEFT JOIN conversion AS c ON p.product_id = c.product_id
LEFT JOIN fluff_products_double_group_id AS dg ON p.product_id = dg.product_id
LEFT JOIN doubles_list AS dl ON dl.product_double_group_id = dg.product_double_group_id