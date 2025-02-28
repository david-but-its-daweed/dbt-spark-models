{{ config(
    schema='recom',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    partition_by=['partition_date'],
    file_format='parquet',
    meta = {
      'model_owner' : '@itangaev',
      'team': 'recom',
      'bigquery_load': 'false',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
WITH previews AS (
    SELECT
        partition_date,
        device_id,
        payload.productId AS product_id,
        FIRST(DATE(FROM_UNIXTIME(event_ts / 1000))) AS event_date,
        lastContext.requestId AS requestId,
        FIRST(lastContext.name) AS context_name,
        FIRST(lastContext.position) AS position,
        FIRST(lastContext.adtechPromoted) AS is_adtech,
        FIRST(experiments) AS experiments
    FROM {{ source('mart', 'device_events') }}
    WHERE partition_date >= DATE'{{ var("start_date_ymd") }}' AND partition_date <= DATE'{{ var("end_date_ymd") }}'
      AND type = "productPreview"
    GROUP BY device_id, product_id, partition_date, requestId
),

clicks AS (
    SELECT
        partition_date,
        device_id,
        payload.productId AS product_id,
        FIRST(DATE(FROM_UNIXTIME(event_ts / 1000))) AS event_date,
        lastContext.requestId AS requestId,
        MAX(IF(type = "productOpen", 1, 0)) AS has_open,
        MAX(IF(type IN ("productToFavorites", "productToCollection", "productToCart"), 1, 0)) AS has_to_cart_or_favorite,
        MAX(IF(type = "productToCart", 1, 0)) AS has_to_cart,
        MAX(IF(type = "productPurchase", 1, 0)) AS has_purchase,
        MAX(IF(type = "productActionClick" AND payload.customizationType = "dislike", 1, 0)) AS has_dislike,
        MAX(IF(type = "productActionClick" AND payload.customizationType = "like", 1, 0)) AS has_like
    FROM {{ source('mart', 'device_events') }}
    WHERE partition_date >= DATE'{{ var("start_date_ymd") }}' AND partition_date <= DATE'{{ var("end_date_ymd") }}'
      AND type IN ("productOpen", "productToFavorites", "productToCollection", "productActionClick", "productToCart", "productPurchase")
    GROUP BY device_id, product_id, partition_date, requestId
),

categories AS (
    SELECT
        category_id,
        name AS category_name
    FROM {{ source('mart', 'category_levels') }}
    WHERE is_leaf = TRUE
),

product_info AS (
    SELECT
        prod.product_id,
        prod.category_id,
        COALESCE(cat.category_name, '') AS category_name,
        prod.created_date
    FROM {{ source('mart', 'published_products_current') }} AS prod
    LEFT JOIN categories AS cat
        ON prod.category_id = cat.category_id
),

device_info AS (
    SELECT
        device_id,
        date_msk,
        top_country_code,
        is_new_device
    FROM {{ ref('active_devices') }}
    WHERE date_msk >= DATE'{{ var("start_date_ymd") }}'
),

joined_data AS (
    SELECT
        p.*,
        COALESCE(c.has_open, 0) AS has_open,
        COALESCE(c.has_to_cart_or_favorite, 0) AS has_to_cart_or_favorite,
        COALESCE(c.has_to_cart, 0) AS has_to_cart,
        COALESCE(c.has_purchase, 0) AS has_purchase,
        COALESCE(c.has_dislike, 0) AS has_dislike,
        COALESCE(c.has_like, 0) AS has_like,
        pi.category_id,
        pi.category_name,
        pi.created_date,
        COALESCE(d.is_new_device, FALSE) AS is_new_device,
        COALESCE(d.top_country_code, 'Other') AS top_country_code,
        IF(DATEDIFF(p.event_date, pi.created_date) < 90, 1, 0) AS is_product_created_less_than_90_days_ago,
        IF(DATEDIFF(p.event_date, pi.created_date) < 365, 1, 0) AS is_product_created_less_than_365_days_ago
    FROM previews AS p
    LEFT JOIN clicks AS c
        ON p.device_id = c.device_id
        AND p.product_id = c.product_id
        AND p.partition_date = c.partition_date
        AND p.requestId = c.requestId
    LEFT JOIN product_info AS pi
        ON p.product_id = pi.product_id
    LEFT JOIN device_info AS d
        ON p.device_id = d.device_id
        AND p.event_date = d.date_msk
)

SELECT * FROM joined_data;