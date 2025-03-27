{{
  config(
    materialized='table',
    alias='1688_products_price_index',
    schema='category_management',
    file_format='parquet',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH products AS (
    SELECT DISTINCT joom_product_id
    FROM {{ source('mi_analytics','ali1688_joom_target_prices') }}
),

kams AS (
    SELECT
        merchant_id,
        import_date,
        CONCAT_WS(',', COLLECT_LIST(DISTINCT kam_name)) AS kams
    FROM {{ source('merchant','kam') }}
    WHERE import_date >= '2025-03-20' -- начало парсинга PI 1688 c таргетной ценой
    GROUP BY 1, 2
),

labels AS (
    SELECT
        partition_date,
        product_id,
        ARRAY_CONTAINS(ARRAY_AGG(label), 'joom_select') AS is_js,
        ARRAY_CONTAINS(ARRAY_AGG(label), 'fbj_more_1d_stock') AS is_fbj
    FROM {{ source('goods','product_labels') }}
    WHERE
        product_id IN (SELECT p.joom_product_id FROM products AS p)
        AND partition_date >= '2025-03-20'
    GROUP BY 1, 2
),

orders AS (
    SELECT DISTINCT product_variant_id
    FROM {{ ref('gold_orders') }}
    WHERE
        product_id IN (SELECT p.joom_product_id FROM products AS p)
        AND order_date_msk >= '2025-03-20'
)

SELECT DISTINCT
    pi1688.partition_date,

    pi1688.joom_product_id AS product_id,
    pi1688.joom_variant_id AS variant_id,

    pi1688.joom_merchant_price AS merchant_price,
    pi1688.ali1688_min_b2b_price AS 1688_min_price,
    pi1688.ali1688_median_b2b_price AS 1688_median_price,
    pi1688.ali1688_target_price AS 1688_target_price,

    pi1688.merchant_price_joom_to_1688_target_price_rate AS price_index,

    p.merchant_id,
    p.store_id,

    k.kams,

    m.origin_name,

    p.business_line,

    mc.l1_merchant_category_name,
    mc.l2_merchant_category_name,
    mc.l3_merchant_category_name,
    mc.l4_merchant_category_name,
    mc.l5_merchant_category_name,

    (pi1688.joom_variant_id IN (SELECT o.product_variant_id FROM orders AS o)) AS has_sales,

    l.is_js AS is_js_approved,
    l.is_fbj
FROM
    {{ source('mi_analytics','ali1688_joom_target_prices') }} AS pi1688
LEFT JOIN
    {{ ref('gold_products') }} AS p ON pi1688.joom_product_id = p.product_id
LEFT JOIN
    {{ ref('gold_merchants') }} AS m ON p.merchant_id = m.merchant_id
LEFT JOIN
    {{ ref('gold_merchant_categories') }} AS mc
    ON p.merchant_category_id = COALESCE(mc.l5_merchant_category_id, mc.l4_merchant_category_id, mc.l3_merchant_category_id, mc.l2_merchant_category_id, mc.l1_merchant_category_id)
LEFT JOIN
    kams AS k
    ON
        p.merchant_id = k.merchant_id
        AND pi1688.partition_date = k.import_date
LEFT JOIN
    labels AS l
    ON
        pi1688.joom_product_id = l.product_id
        AND pi1688.partition_date = l.partition_date