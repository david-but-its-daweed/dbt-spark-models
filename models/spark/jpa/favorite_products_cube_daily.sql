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
        MIN(partition_date) OVER (PARTITION BY id, product_id, shop_id) as min_pt,
        MAX(partition_date) OVER (PARTITION BY id, product_id, shop_id) as max_pt,
        p.partition_date = MAX(partition_date) OVER (PARTITION BY id, product_id, shop_id) AS is_current
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
        min_pt,
        max_pt,
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
),

dates AS (
  SELECT
    DISTINCT
    EXPLODE(sequence(TO_DATE(min_pt), TO_DATE(max_pt), interval '1' day)) as partition_date,
    id,
    product_id,
    shop_id
  FROM kube k
)

SELECT
    dates.partition_date,
    COALESCE(kube.is_current, FALSE),
    dates.id,
    dates.product_id,
    dates.shop_id,
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
    LAST_VALUE(CASE WHEN kube.price_amount IS NOT NULL THEN kube.price_amount END) OVER (PARTITION BY id ORDER BY dates.partition_date ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW) AS price_amount,
    LAST_VALUE(CASE WHEN kube.price_amount_1da IS NOT NULL THEN kube.price_amount_1da END) OVER (PARTITION BY id ORDER BY dates.partition_date ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW) AS price_amount_1da,
    LAST_VALUE(CASE WHEN p.original_price_amount IS NOT NULL THEN p.original_price_amount END) OVER (PARTITION BY id ORDER BY dates.partition_date ROWS BETWEEN
UNBOUNDED PRECEDING AND CURRENT ROW) AS original_price_amount
FROM dates
INNER JOIN {{ source('joompro_analytics_mart', 'mercadolibre_products_snapshot') }} AS p USING (id, product_id, shop_id)
LEFT JOIN kube USING (partition_date, id, product_id, shop_id)
LEFT JOIN {{ source('joompro_mart', 'mercadolibre_shops') }} AS s USING (shop_id)
LEFT JOIN {{ ref('mercadolibre_categories_view') }} AS c USING (category_id)

