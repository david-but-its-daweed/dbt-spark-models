{{ config(
    schema='joompro_analytics_mart',
    materialized='view',
    meta = {
      'model_owner': '@mironov',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false'
    }
) }}

WITH

favorite_product_ids AS (
    SELECT DISTINCT
        productid AS product_id,
        shopid AS shop_id
    FROM {{ source('mongo', 'b2b_core_analytics_favorite_products_daily_snapshot') }}
),

favorite_product_measurements AS (
    SELECT
        p.*,
        p.partition_date = MAX(p.partition_date) OVER () AS is_current
    FROM favorite_product_ids
    INNER JOIN {{ source('joompro_analytics', 'mercadolibre_product_measurements') }} AS p USING (product_id, shop_id)
    WHERE p.partition_date >= ADD_MONTHS(CURRENT_DATE, -1)
),

kube AS (
    SELECT
        partition_date,
        is_current,
        id,
        product_id,
        shop_id,
        orders AS orders_1d,
        LAG(orders, 1) OVER (PARTITION BY id, product_id, shop_id ORDER BY partition_date) AS orders_1da,
        gmv AS gmv_1d,
        LAG(gmv, 1) OVER (PARTITION BY id, product_id, shop_id ORDER BY partition_date) AS gmv_1da,
        reviews_count,
        LAG(reviews_count, 1) OVER (PARTITION BY id, product_id, shop_id ORDER BY partition_date) AS reviews_count_1da,
        reviews_rating,
        LAG(reviews_rating, 1) OVER (PARTITION BY id, product_id, shop_id ORDER BY partition_date) AS reviews_rating_1da,
        price_amount,
        LAG(price_amount, 1) OVER (PARTITION BY id, product_id, shop_id ORDER BY partition_date) AS price_amount_1da
    FROM favorite_product_measurements
)

SELECT
    kube.partition_date,
    kube.is_current,
    p.id,
    p.product_id,
    p.shop_id,
    p.title,
    p.image_url,
    p.brand_name,
    s.name AS shop_name,
    s.url AS shop_url,
    c.l1_name,
    c.l1_id,
    c.l2_name,
    c.l2_id,
    c.l3_name,
    c.l3_id,
    c.l4_name,
    c.l4_id,
    c.l5_name,
    c.l5_id,
    c.l6_name,
    c.l6_id,
    c.l7_name,
    c.l7_id,
    COALESCE(kube.orders_1d, 0) AS orders_1d,
    COALESCE(kube.orders_1da, 0) AS orders_1da,
    COALESCE(kube.gmv_1d, 0) AS gmv_1d,
    COALESCE(kube.gmv_1da, 0) AS gmv_1da,
    COALESCE(kube.reviews_count, 0) AS reviews_count,
    COALESCE(kube.reviews_count_1da, 0) AS reviews_count_1da,
    COALESCE(kube.reviews_rating, 0) AS reviews_rating,
    COALESCE(kube.reviews_rating_1da, 0) AS reviews_rating_1da,
    COALESCE(kube.price_amount, 0) AS price_amount,
    COALESCE(kube.price_amount_1da, 0) AS price_amount_1da,
    COALESCE(p.original_price_amount, 0) AS original_price_amount
FROM kube
INNER JOIN {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }} AS p USING (product_id, shop_id)
LEFT JOIN {{ source('joompro_mart', 'mercadolibre_shops') }} AS s USING (shop_id)
LEFT JOIN {{ ref('mercadolibre_categories_view') }} AS c USING (category_id)
WHERE
    p.product_id IS NOT NULL
    AND p.id IS NOT NULL
    AND p.shop_id IS NOT NULL
    AND p.category_id IS NOT NULL
