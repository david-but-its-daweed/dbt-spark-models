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

WITH products_raw AS (
    SELECT
        product_id,
        merchant_id,
        product_name,
        COALESCE(is_public AND NOT archived AND NOT removed, FALSE) AS is_available,
        labels
    FROM {{ source('mart', 'published_products_current') }}
),

products_exploded AS (
    SELECT
        product_id,
        merchant_id,
        product_name,
        is_available,
        l.key AS label_key
    FROM products_raw
        LATERAL VIEW EXPLODE(labels) AS l
),

products AS (
    SELECT
        product_id,
        merchant_id,
        product_name,
        is_available,
        COLLECT_SET(label_key) AS labels
    FROM products_exploded
    GROUP BY product_id, merchant_id, product_name, is_available
    HAVING ARRAY_CONTAINS(labels, 'VerticalStore_Fluff')
),

description_and_merchant_name AS (
    SELECT
        product_id,
        merchant_name,
        orig_description
    FROM {{ source('mart', 'dim_published_product_with_merchant') }}
    WHERE TO_DATE(next_effective_ts) > '3000-01-01'
),

kams AS (
    SELECT DISTINCT
        merchant_id,
        FIRST_VALUE(kam_name) OVER (PARTITION BY merchant_id ORDER BY import_date DESC, kam_name ASC) AS kam_name
    FROM {{ source('merchant', 'kam') }}
),

orders AS (
    SELECT
        product_id,
        SUM(IF(app_entity = 'fluff', 1, 0)) AS fluff_orders_count,
        ROUND(AVG(IF(app_entity = 'fluff' AND refund_reason IN ('other', 'fraud', 'quality', 'not_delivered'), 1, CAST(NULL AS INT)))) AS fluff_refund_rate,
        ROUND(AVG(IF(app_entity = 'fluff' AND refund_reason IN ('cancelled_by_customer', 'cancelled_by_merchant'), 1, CAST(NULL AS INT)))) AS fluff_canceled_rate,
        SUM(IF(app_entity = 'joom' AND partition_date >= '2025-03-03', 1, 0)) AS joom_orders_count_since_03_03,
        ROUND(
            AVG(
                IF(
                    app_entity = 'joom'
                    AND partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
                    AND refund_reason IN ('other', 'fraud', 'quality', 'not_delivered'),
                    1,
                    CAST(NULL AS INT)
                )
            )
        ) AS joom_refund_rate_last_3_month,
        ROUND(
            AVG(
                IF(
                    app_entity = 'joom'
                    AND partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
                    AND refund_reason IN ('cancelled_by_customer', 'cancelled_by_merchant'),
                    1,
                    CAST(NULL AS INT)
                )
            )
        ) AS joom_canceled_rate_last_3_month
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE
        partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
        OR partition_date >= '2025-03-03'
    GROUP BY product_id
),

fbj_stock AS (
    SELECT
        product_id,
        SUM(number_of_products_in_stock) AS fbj_number_of_products_in_stock,
        COLLECT_LIST(NAMED_STRUCT('product_variant_id', product_variant_id, 'number_of_products_in_stock', number_of_products_in_stock)) AS fbj_number_of_products_in_stock_by_variant_id
    FROM {{ ref('fbj_product_stocks_current') }}
    GROUP BY product_id
),

dublicate_group_id_by_product AS (
    SELECT
        productId AS product_id,
        groupId AS product_double_group_id,
        -- COLLECT_LIST(productId) OVER (PARTITION BY groupId) AS duplicate_product_ids_list
    FROM {{ source('default', 'productsGroups_louvain') }}
),

pi_raw AS (
    SELECT
        joom_product_id AS product_id,
        partition_date,
        full_price_with_vat_joom_to_aliexpress_rate,
        special_price_with_vat_joom_to_aliexpress_rate
    FROM {{ source('mi_analytics', 'aliexpress_joom_automatching_price_index') }}
    WHERE partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
),

pi AS (
    SELECT
        product_id,
        ROUND(PERCENTILE_APPROX(full_price_with_vat_joom_to_aliexpress_rate, 0.5), 1) AS price_index,
        ROUND(PERCENTILE_APPROX(special_price_with_vat_joom_to_aliexpress_rate, 0.5), 1) AS sp_price_index
    FROM pi_raw
    GROUP BY product_id
),

order_app_entity AS (
    SELECT
        order_id,
        app_entity
    FROM {{ source('mart', 'star_order_2020') }}
    WHERE partition_date >= CURRENT_DATE() - INTERVAL '92' DAY
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
    LEFT JOIN order_app_entity AS oa ON lo.order_id = oa.order_id
    GROUP BY lo.product_id
),

eu_countries AS (
    SELECT EXPLODE(country_codes) AS country
    FROM {{ source('mart', 'dim_region') }}
    WHERE
        next_effective_ts > '3000-01-01'
        AND region = 'EU'
),

eu_available AS (
    SELECT DISTINCT
        pp.product_id,
        1 AS is_available_in_eu
    FROM {{ source('price', 'preview_prices') }} AS pp
    INNER JOIN eu_countries AS ec ON pp.country = ec.country
    WHERE
        pp.partition_date > CURRENT_DATE() - INTERVAL '92' DAY
),

conversion_raw AS (
    SELECT
        device_id,
        payload.productId AS product_id,
        MAX(device.pref_country) AS pref_country,
        MAX(device.os_type) AS os_type,
        MAX(IF(type = 'productPreview', 1, 0)) AS productPreview,
        MAX(IF(type = 'productOpenServer', 1, 0)) AS productOpenServer,
        MAX(IF(type = 'productToCart', 1, 0)) AS productToCart,
        MAX(IF(type = 'productPurchase', 1, 0)) AS productPurchase
    FROM {{ source('mart', 'device_events') }}
    WHERE
        partition_date >= '2025-03-03'
        AND device.app_entity = 'fluff'
        AND ephemeral = FALSE
        AND type IN ('productPreview', 'productOpenServer', 'productToCart', 'productPurchase')
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
    dm.merchant_name,
    dm.orig_description,
    k.kam_name,
    o.fluff_orders_count,
    o.fluff_refund_rate,
    o.fluff_canceled_rate,
    o.joom_orders_count_since_03_03,
    o.joom_refund_rate_last_3_month,
    o.joom_canceled_rate_last_3_month,
    fs.fbj_number_of_products_in_stock,
    fs.fbj_number_of_products_in_stock_by_variant_id,
    dg.product_double_group_id,
    -- dg.duplicate_product_ids_list,
    pi.price_index,
    pi.sp_price_index,
    lo.fluff_fulfilling_days_last_3_month_80th_perc,
    lo.joom_fulfilling_days_last_3_month_80th_perc,
    COALESCE(eu.is_available_in_eu, 0) AS is_available_in_eu,
    c.preview_count,
    c.preview_to_open_perc,
    c.open_count,
    c.open_to_cart_perc,
    c.to_cart_count,
    c.cart_to_purchase_perc,
    c.purchase_count
FROM products AS p
LEFT JOIN kams AS k ON p.merchant_id = k.merchant_id
LEFT JOIN description_and_merchant_name AS dm ON p.product_id = dm.product_id
LEFT JOIN orders AS o ON p.product_id = o.product_id
LEFT JOIN fbj_stock AS fs ON p.product_id = fs.product_id
LEFT JOIN dublicate_group_id_by_product AS dg ON p.product_id = dg.product_id
LEFT JOIN pi ON p.product_id = pi.product_id
LEFT JOIN logistics_order AS lo ON p.product_id = lo.product_id
LEFT JOIN eu_available AS eu ON p.product_id = eu.product_id
LEFT JOIN conversion AS c ON p.product_id = c.product_id
