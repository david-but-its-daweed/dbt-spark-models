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

WITH kams AS (
    SELECT DISTINCT
        merchant_id,
        FIRST_VALUE(kam_name) OVER (PARTITION BY merchant_id ORDER BY import_date DESC, kam_name ASC) AS kam_name
    FROM {{ source('merchant', 'kam') }}
),

products_raw AS (
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
)

SELECT
    p.product_id,
    p.merchant_id,
    p.product_name,
    p.is_available,
    MAX(k.kam_name) AS kam_name,
    COLLECT_SET(p.label_key) AS labels
FROM products_exploded AS p
LEFT JOIN kams AS k ON p.merchant_id = k.merchant_id
GROUP BY p.product_id, p.merchant_id, p.product_name, p.is_available
HAVING ARRAY_CONTAINS(labels, 'VerticalStore_Fluff')