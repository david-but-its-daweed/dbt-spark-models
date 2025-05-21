{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@kirill_melnikov',
      'bigquery_load': 'true',
    }
) }}

WITH categories AS (
    SELECT
        category_id,
        name AS category_name,
        level_1_category.name AS level_1_category_name,
        level_2_category.name AS level_2_category_name,
        level_3_category.name AS level_3_category_name,
        level_1_category.id AS level_1_category_id,
        level_2_category.id AS level_2_category_id,
        level_3_category.id AS level_3_category_id
    FROM {{ source('mart', 'category_levels') }}
),

ali1688 AS (
    SELECT
        _id AS b2b_product_id,
        IF(extId IS NOT NULL, SPLIT(extId, 'ali1688/')[1], NULL) AS ali1688_product_id
    FROM {{ source('mongo', 'b2b_core_product_appendixes_daily_snapshot') }}
),

matching AS (
    SELECT DISTINCT
        ali1688.b2b_product_id AS product_id,
        ali1688.ali1688_product_id,

        joom_ids.joom_product_id
    FROM
        ali1688
    LEFT JOIN
        {{ source('productsmatching', 'joom_1688_product_variant_matches') }} AS joom_ids
        ON ali1688.ali1688_product_id = joom_ids.ali_1688_product_id
)

SELECT
    pp._id AS product_id,

    m.ali1688_product_id,
    m.joom_product_id,

    pp.categoryId AS category_id,
    cat.category_name,
    cat.level_1_category_name,
    cat.level_2_category_name,
    cat.level_3_category_name,
    cat.level_1_category_id,
    cat.level_2_category_id,
    cat.level_3_category_id,

    MILLIS_TO_TS_MSK(pp.createdTimeMs) AS created_ts_msk,
    DATE(MILLIS_TO_TS_MSK(pp.createdTimeMs)) AS created_date,

    pp.dangerousKind AS dangerous_kind,

    pp.merchantId AS merchant_id,

    pp.origDescription AS orig_description,
    pp.origExtraImageUrls AS orig_extra_image_urls,
    pp.origMainImageUrl AS orig_main_image_url,
    pp.origName AS orig_name,
    pp.origUrl AS orig_url,

    pp.sku,
    pp.storeId AS store_id,

    IF(pp.merchantId = '66054380c33acc34a54a56d0', TRUE, FALSE) AS is_ali1688_product,

    MILLIS_TO_TS_MSK(pp.updatedTimeMs) AS update_ts_msk,
    DATE(MILLIS_TO_TS_MSK(pp.updatedTimeMs)) AS update_date,
    MILLIS_TO_TS_MSK(pp.publishedTimeMs) AS published_ts_msk,
    DATE(MILLIS_TO_TS_MSK(pp.publishedTimeMs)) AS published_date
FROM {{ ref('scd2_published_products_snapshot') }} AS pp
LEFT JOIN categories AS cat ON pp.categoryId = cat.category_id
LEFT JOIN matching AS m ON pp._id = m.product_id
WHERE
    pp.dbt_valid_to IS NULL