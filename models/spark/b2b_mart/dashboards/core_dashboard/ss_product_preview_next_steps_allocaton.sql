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
    NULL AS with_move_to_deal,
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

UNION ALL

(
    WITH add_data AS (
        SELECT
            user_id,
            event_ts_msk,
            actionType,
            product_id
        FROM {{ ref('ss_events_cart') }}
        WHERE actionType IN ('add_to_cart', 'move_to_deal')
        GROUP BY all
    ),

    prepare AS (
        SELECT
            *,
            LAG(actionType) OVER (PARTITION BY user_id, product_id ORDER BY TIMESTAMP(event_ts_msk)) AS actionTypel,
            LEAD(actionType) OVER (PARTITION BY user_id, product_id ORDER BY TIMESTAMP(event_ts_msk)) AS actionType_lead
        FROM add_data
        WHERE actionType IN ('add_to_cart', 'move_to_deal')
    ),

    add_to_c AS (
        SELECT
            user_id,
            event_ts_msk,
            actionType,
            product_id,
            CASE WHEN actionType_lead = 'move_to_deal' THEN 1 ELSE 0 END AS move_to_deal
        FROM prepare
        WHERE (actionType = 'add_to_cart' AND actionTypel IS NULL)
    ),

    previous AS (
        SELECT j.*
        FROM {{ source('b2b_mart', 'ss_events_customer_journey') }} AS j
        INNER JOIN (
            SELECT
                user_id,
                product_id
            FROM add_to_c
        ) AS a ON j.user_id = a.user_id AND j.product_id = a.product_id
        WHERE j.type = 'productPreview'
    ),

    all_data AS (
        SELECT
            user_id,
            device_platform,
            type,
            product_id,
            event_ts_msk,
            pageUrl,
            pageName,
            position,
            NULL AS move_to_deal
        FROM previous
        UNION ALL
        SELECT
            user_id,
            NULL AS device_platform,
            actionType AS type,
            product_id,
            event_ts_msk,
            NULL AS pageUrl,
            NULL AS pageName,
            NULL AS position,
            move_to_deal
        FROM add_to_c
    ),

    data_prep AS (
        SELECT
            all_data.*,
            CAST(TIMESTAMP(event_ts_msk) AS DATE) AS event_msk_date,
            LEAD(type) OVER (PARTITION BY user_id, product_id ORDER BY event_ts_msk) AS next_type,
            LEAD(move_to_deal) OVER (PARTITION BY user_id, product_id ORDER BY event_ts_msk) AS with_move_to_deal
        FROM all_data
        WHERE CAST(TIMESTAMP(event_ts_msk) AS DATE) >= '2025-01-01'
    ),

    p AS (
        SELECT
            *,
            CASE
                WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%q.%' THEN 'query_search'
                WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%i.%' THEN 'image_search'
                WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%c.%' THEN 'category'
                WHEN pageUrl LIKE '%/search%' THEN 'catalog'
                WHEN pageUrl LIKE '%/products/%' THEN 'product'
                WHEN pageUrl LIKE '%promotions%' THEN 'promotions'
                WHEN pageUrl LIKE '%/search/%' AND pageUrl LIKE '%c.%' AND (position != '' AND position IS NOT NULL AND position != 'catalog') THEN 'banner_category'
                WHEN pageUrl LIKE '%/search%' AND (position IS NOT NULL OR position = '' OR LOWER(position) != 'catalog') THEN 'banner_catalog'
                ELSE 'other'
            END AS source,
            with_move_to_deal,
            CASE WHEN position = 'catalog' OR position IS NULL THEN '' ELSE position END AS add_source
        FROM data_prep
        WHERE next_type = 'add_to_cart' AND type = 'productPreview'
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
        with_move_to_deal,
        NULL AS next_pageName,
        next_type,
        source
    FROM p
)
