{{
  config(
    materialized='table',
    alias='customer_categories',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}

WITH main AS (
    SELECT
        _id AS customer_category_id,
        name AS customer_category_name,
        parentid AS parent_category_id
    FROM {{ source('mongo', 'abu_core_catalog_daily_snapshot') }}
),

l1_categories AS (
    SELECT DISTINCT *
    FROM main
    WHERE parent_category_id IS NULL
),

l2_categories AS (
    SELECT DISTINCT main.*
    FROM l1_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
),

l3_categories AS (
    SELECT DISTINCT main.*
    FROM l2_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
),

l4_categories AS (
    SELECT DISTINCT main.*
    FROM l3_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
),

l5_categories AS (
    SELECT DISTINCT main.*
    FROM l4_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
),

l6_categories AS (
    SELECT DISTINCT main.*
    FROM l5_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
),

l7_categories AS (
    SELECT DISTINCT main.*
    FROM l6_categories AS a
    INNER JOIN main
        ON a.customer_category_id = main.parent_category_id
)

SELECT
    CASE
        WHEN l7_categories.customer_category_id IS NOT NULL
            THEN l7_categories.customer_category_id
        WHEN l6_categories.customer_category_id IS NOT NULL
            THEN l6_categories.customer_category_id
        WHEN l5_categories.customer_category_id IS NOT NULL
            THEN l5_categories.customer_category_id
        WHEN l4_categories.customer_category_id IS NOT NULL
            THEN l4_categories.customer_category_id
        WHEN l3_categories.customer_category_id IS NOT NULL
            THEN l3_categories.customer_category_id
        WHEN l2_categories.customer_category_id IS NOT NULL
            THEN l2_categories.customer_category_id
        WHEN l1_categories.customer_category_id IS NOT NULL
            THEN l1_categories.customer_category_id
    END AS customer_category_id,

    CASE
        WHEN l7_categories.customer_category_id IS NOT NULL
            THEN l7_categories.customer_category_name
        WHEN l6_categories.customer_category_id IS NOT NULL
            THEN l6_categories.customer_category_name
        WHEN l5_categories.customer_category_id IS NOT NULL
            THEN l5_categories.customer_category_name
        WHEN l4_categories.customer_category_id IS NOT NULL
            THEN l4_categories.customer_category_name
        WHEN l3_categories.customer_category_id IS NOT NULL
            THEN l3_categories.customer_category_name
        WHEN l2_categories.customer_category_id IS NOT NULL
            THEN l2_categories.customer_category_name
    END AS customer_category_name,

    l1_categories.customer_category_id AS l1_customer_category_id,
    l1_categories.customer_category_name AS l1_customer_category_name,

    l2_categories.customer_category_id AS l2_customer_category_id,
    l2_categories.customer_category_name AS l2_customer_category_name,

    l3_categories.customer_category_id AS l3_customer_category_id,
    l3_categories.customer_category_name AS l3_customer_category_name,

    l4_categories.customer_category_id AS l4_customer_category_id,
    l4_categories.customer_category_name AS l4_customer_category_name,

    l5_categories.customer_category_id AS l5_customer_category_id,
    l5_categories.customer_category_name AS l5_customer_category_name,

    l6_categories.customer_category_id AS l6_customer_category_id,
    l6_categories.customer_category_name AS l6_customer_category_name,

    l7_categories.customer_category_id AS l7_customer_category_id,
    l7_categories.customer_category_name AS l7_customer_category_name
FROM l1_categories
LEFT JOIN l2_categories
    ON l1_categories.customer_category_id = l2_categories.parent_category_id
LEFT JOIN l3_categories
    ON l2_categories.customer_category_id = l3_categories.parent_category_id
LEFT JOIN l4_categories
    ON l3_categories.customer_category_id = l4_categories.parent_category_id
LEFT JOIN l5_categories
    ON l4_categories.customer_category_id = l5_categories.parent_category_id
LEFT JOIN l6_categories
    ON l5_categories.customer_category_id = l6_categories.parent_category_id
LEFT JOIN l7_categories
    ON l6_categories.customer_category_id = l7_categories.parent_category_id
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14