{{
  config(
    materialized='table',
    alias='products',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}

WITH orders AS (
    SELECT
        product_id,
        SUM(product_quantity) AS total_number_of_purchases,
        MAX(order_datetime_utc) AS last_purchase_datetime_utc,
        MIN(current_merchant_sale_price) AS current_merchant_sale_price
    FROM (
        SELECT
            *,
            LAST_VALUE(merchant_sale_price) OVER (PARTITION BY product_id ORDER BY order_datetime_utc) AS current_merchant_sale_price
        FROM {{ ref('gold_orders') }}
    )
    GROUP BY 1
)

SELECT
    products.product_id,
    products.store_id,
    products.brand_id,
    products.merchant_id,

    products.category_id AS merchant_category_id,
    categories.l1_mechant_category_id,
    categories.l1_mechant_category_name,
    categories.l2_mechant_category_id,
    categories.l2_mechant_category_name,

    products.product_name,
    products.brand_name,
    products.image_url,

    orders.last_purchase_datetime_utc,
    orders.total_number_of_purchases,

    orders.current_merchant_sale_price,

    products.is_public,
    products.created_date AS created_date_utc,
    products.rating AS product_rating
FROM {{ source('mart', 'published_products_current') }} AS products
LEFT JOIN orders USING (product_id)
LEFT JOIN {{ ref('gold_merchant_categories') }} AS categories
    ON categories.merchant_category_id = products.category_id
