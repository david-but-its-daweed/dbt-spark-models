{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@tigran',
      'bigquery_load': 'true'
    }
) }}

WITH prep AS (
    SELECT
        user.userId AS user_id,
        event_ts_msk,
        partition_date AS event_msk_date,
        coalesce(payload.productId, replace(payload.pageUrl,'https://joom.pro/pt-br/products/','')) AS product_id,
        payload.pageUrl AS pageUrl,
        type,
        payload.position,
        payload.pageName
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE
        partition_date >= '2024-04-01'
        AND (
            type = 'productPreview'
            OR (type = 'pageView' AND payload.pageName = 'product')
        )
),

base AS (
    SELECT
        user_id,
        event_ts_msk,
        event_msk_date,
        product_id,
        pageUrl,
        type,
        position,
        pageName,
        LEAD(pageName) OVER (PARTITION BY user_id, product_id ORDER BY event_ts_msk) AS next_pageName,
        LEAD(type) OVER (PARTITION BY user_id, product_id ORDER BY event_ts_msk) AS next_type
    FROM prep
)

SELECT
    user_id,
    event_ts_msk,
    event_msk_date,
    product_id,
    pageUrl,
    type,
    position,
    pageName,
    next_pageName,
    next_type,
    CASE
        WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%q.%' THEN 'query_search'
        WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%i.%' THEN 'image_search'
        WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%c.%' AND (position != '' AND position IS NOT NULL AND position != 'catalog') THEN 'banner_category'
        WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%c.%' THEN 'category'
        WHEN pageUrl LIKE '%/search%' AND (position != '' AND position IS NOT NULL AND position != 'catalog') THEN 'banner_catalog'
        WHEN pageUrl LIKE '%/search%' THEN 'catalog'
        WHEN pageUrl LIKE '%/products/%' THEN 'product'
        WHEN pageUrl LIKE '%promotions%' THEN 'promotions'
        ELSE 'other'
    END AS source
FROM base
WHERE next_type = 'pageView' AND next_pageName = 'product' AND type = 'productPreview'
