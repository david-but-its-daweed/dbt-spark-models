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
        DATE(e.event_time) AS partition_date,
        user_id,
        FROM_JSON(e.event_params, 'product_id STRING').product_id AS product_id,
        e.event_type AS event_type,
    
        CASE
            WHEN
                e.event_type = 'addToCart' AND LAG(e.event_type) OVER (PARTITION BY user_id, FROM_JSON(e.event_params, 'product_id STRING').product_id ORDER BY e.event_time) != 'addToCart'
                THEN
                    LAG(FROM_JSON(e.event_params, 'pageUrl STRING').pageUrl) OVER (PARTITION BY user_id, FROM_JSON(e.event_params, 'product_id STRING').product_id ORDER BY e.event_time)
            ELSE FROM_JSON(e.event_params, 'pageUrl STRING').pageUrl
        END AS page_url
    FROM 
        b2b_mart.ss_events_by_session
        LATERAL VIEW EXPLODE(events_in_session) AS e
    WHERE
        e.event_type IN ('productPreview', 'productClick', 'addToCart')
        AND DATE(e.event_time) >= '2025-03-01'
),

events AS (
    SELECT
        partition_date,
        user_id,

        merchant_id,
        merchant_name,
        p.product_id,
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
    INNER JOIN products AS p ON re.product_id = p.product_id 
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
    LEFT JOIN {{ source('b2b_mart', 'fact_deals_with_requests') }} AS fdwr ON fcr.deal_id = fdwr.deal_id
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
        (ut.user_id IS NOT NULL
        OR
        s.user_id IN ( -- наши тестовые юзеры
            '65f8a3b040640e6f0b103c62',
            '62a9feec98d5f1bcd5f8f651',
            '64dfe85752e94057726ce7e3',
            '654a36ca194414a2aa015942',
            '6571e7a767653caa48078a8b',
            '6050ddece1fffe0006ee7d80',
            '625441434c41263737ad2ca4',
            '65e8783880017584d8a361f6',
            '65ba98813b6d7111865f2f91',
            '661425270c35c69b50009cb0'
            ) 
        ) AS is_registrated
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