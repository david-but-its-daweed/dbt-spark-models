{{
  config(
    materialized='table',
    alias='products',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_override_dataset_id': 'gold_migration',
        'priority_weight': '150',
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
    a.product_id,
    a.category_id AS merchant_category_id,
    a.store_id,
    a.brand_id,
    a.merchant_id,

    a.product_name,
    a.brand_name,
    a.image_url,

    b.last_purchase_datetime_utc,
    b.total_number_of_purchases,

    b.current_merchant_sale_price,

    a.is_public,
    a.created_date AS created_date_utc,
    a.rating AS product_rating
FROM {{ source('mart', 'published_products_current') }} AS a
LEFT JOIN orders AS b USING (product_id)
