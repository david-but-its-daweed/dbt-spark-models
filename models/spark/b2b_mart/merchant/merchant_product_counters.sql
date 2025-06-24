{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

-- Создание календаря через последовательность дат
WITH calendar_dt AS (
    SELECT
        EXPLODE(
            SEQUENCE(
                DATE('2025-03-01'),
                CURRENT_DATE()
            )
        ) AS partition_date
),

products AS (
    SELECT DISTINCT
        c.partition_date,
        mp.merchant_id,
        mp.product_id
    FROM {{ source('b2b_mart', 'merchant_products') }} AS mp
    CROSS JOIN calendar_dt AS c
),

stats AS (
    SELECT
        cjm.event_msk_date,

        cjm.product_id,
        ap.category_name,
        ap.level_1_category_name,
        ap.level_2_category_name,
        ap.level_3_category_name,

        COUNT(DISTINCT IF(cjm.type = 'productPreview', cjm.user_id, NULL)) AS preview,
        COUNT(DISTINCT IF(cjm.type = 'productClick', cjm.user_id, NULL)) AS open,
        COUNT(DISTINCT IF(cjm.type = 'addToCart', cjm.user_id, NULL)) AS add_to_cart
    FROM {{ source('b2b_mart', 'ss_events_customer_journey') }} AS cjm
    INNER JOIN {{ source('b2b_mart', 'ss_assortment_products') }} AS ap ON cjm.product_id = ap.product_id
    WHERE
        cjm.type IN ('productPreview', 'productClick', 'addToCart')
        AND cjm.event_msk_date >= DATE('2025-03-01')
    GROUP BY
        cjm.event_msk_date, cjm.product_id,
        ap.category_name,
        ap.level_1_category_name,
        ap.level_2_category_name,
        ap.level_3_category_name
),

deals AS (
    SELECT
        CASE
            WHEN link LIKE 'https://joom.pro/pt-br/products/%' THEN REGEXP_EXTRACT(link, 'https://joom.pro/pt-br/products/(.*)', 1)
            ELSE link
        END AS product_id,
        COUNT(DISTINCT deal_id) AS deals,
        COUNT(DISTINCT customer_request_id) AS requests
    FROM {{ source('b2b_mart', 'fact_customer_requests') }}
    WHERE country IN ('BR', 'MX') AND (link LIKE 'https://joom.pro/pt-br/products/%' OR link LIKE '6%')
    GROUP BY
        CASE
            WHEN link LIKE 'https://joom.pro/pt-br/products/%' THEN REGEXP_EXTRACT(link, 'https://joom.pro/pt-br/products/(.*)', 1)
            ELSE link
        END
)

SELECT
    p.partition_date,

    p.merchant_id,
    p.product_id,

    COALESCE(s.preview, 0) AS preview,
    COALESCE(s.open, 0) AS open,
    COALESCE(s.add_to_cart, 0) AS add_to_cart,
    COALESCE(d.deals, 0) AS deals,
    COALESCE(d.requests, 0) AS requests
FROM products AS p
LEFT JOIN stats AS s
    ON p.product_id = s.product_id AND p.partition_date = s.event_msk_date
LEFT JOIN deals AS d
    ON p.product_id = d.product_id