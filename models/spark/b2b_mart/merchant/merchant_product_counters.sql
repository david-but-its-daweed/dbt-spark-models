{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

WITH products AS (
    SELECT DISTINCT
        merchant_id,
        merchant_name,
        product_id,
        product_name
    FROM {{ source('b2b_mart', 'merchant_products') }}
),

raw_events AS (
    SELECT
        cjm.event_msk_date AS partition_date,
        cjm.user_id,

        p.merchant_id,
        p.merchant_name,
        p.product_id,
        p.product_name,

        cjm.type AS event_type,

        CASE
            WHEN
                cjm.type = 'addToCart' AND LAG(cjm.type) OVER (PARTITION BY cjm.user_id, cjm.product_id ORDER BY cjm.event_ts_msk) != 'addToCart'
                THEN
                    LAG(cjm.pageUrl) OVER (PARTITION BY cjm.user_id, cjm.product_id ORDER BY cjm.event_ts_msk)
            ELSE cjm.pageUrl
        END AS page_url
    FROM {{ source('b2b_mart', 'ss_events_customer_journey') }} AS cjm
    INNER JOIN products AS p ON cjm.product_id = p.product_id
    WHERE
        cjm.type IN ('productPreview', 'productClick', 'addToCart')
        AND cjm.event_msk_date >= '2025-03-01'
),

events AS (
    SELECT
        partition_date,
        user_id,

        merchant_id,
        merchant_name,
        product_id,
        product_name,

        event_type,
        CASE
            WHEN page_url LIKE '%/search/%' AND page_url LIKE '%q.%' THEN 'keyword search'
            WHEN page_url LIKE '%/search/%' AND page_url LIKE '%i.%' THEN 'image search'
            WHEN page_url LIKE '%/search/%' AND page_url LIKE '%c.%' THEN 'category search'
            WHEN page_url LIKE '%/search%' THEN 'catalog'
            WHEN page_url LIKE '%/products/%' THEN 'similar products' 
            WHEN page_url LIKE '%promotions%' THEN 'promotions'
            ELSE 'other'
        END AS event_info
    FROM raw_events AS re
),

deals AS (
    SELECT DISTINCT
        DATE(fcr.created_time) AS partition_date,
        fcr.user_id,

        p.merchant_id,
        p.merchant_name,
        p.product_id,
        p.product_name,

        'deal' AS event_type,
        fdwr.deal_friendly_id AS event_info
    FROM {{ source('b2b_mart', 'fact_customer_requests') }} AS fcr
    INNER JOIN
        products AS p
        ON
            CASE
                WHEN fcr.link LIKE 'https://joom.pro/pt-br/products/%' THEN REGEXP_EXTRACT(fcr.link, 'https://joom.pro/pt-br/products/(.*)', 1)
                ELSE fcr.link
            END = p.product_id
    LEFT JOIN {{ source('b2b_mart', 'fact_deals_with_requests') }} AS fdwr
    WHERE
        fcr.country IN ('BR', 'MX') AND (fcr.link LIKE 'https://joom.pro/pt-br/products/%' OR fcr.link LIKE '6%')
        AND fcr.next_effective_ts_msk IS NULL
        AND DATE(fcr.created_time) >= '2025-03-01'
),

stats AS (
    SELECT * FROM events
    UNION ALL
    SELECT * FROM deals
),

full AS (
    SELECT
        s.*,
        (ut.user_id IS NOT NULL) AS is_registrated
    FROM stats AS s
    LEFT JOIN {{ source('b2b_mart', 'ss_users_table') }} AS ut
    ON
        s.user_id = ut.user_id
        AND s.partition_date >= DATE(ut.registration_start)
)

SELECT
    f.partition_date,
    f.user_id,
    f.is_registrated,

    f.event_type,
    f.event_info,

    f.merchant_id,
    f.merchant_name,
    
    f.product_id,
    f.product_name,

    ap.category_name,
    ap.level_1_category_name,
    ap.level_2_category_name,
    ap.level_3_category_name
FROM full AS f
LEFT JOIN {{ source('b2b_mart', 'ss_assortment_products') }} AS ap ON f.product_id = ap.product_id
