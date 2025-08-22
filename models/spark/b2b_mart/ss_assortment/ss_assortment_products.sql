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
    FROM {{ source('mongo', 'b2b_product_product_appendixes_daily_snapshot') }}
),

matching AS (
    SELECT
        ali1688.b2b_product_id AS product_id,
        COLLECT_SET(ali1688.ali1688_product_id) AS ali1688_product_id,
        COLLECT_SET(joom_ids.joom_product_id) AS joom_product_id
    FROM
        ali1688
    LEFT JOIN
        {{ source('productsmatching', 'joom_1688_product_variant_matches') }} AS joom_ids
        ON ali1688.ali1688_product_id = joom_ids.ali_1688_product_id
    GROUP BY 1
),

product_states AS (
    SELECT
        product_id,

        CASE
            WHEN status = 1 THEN 'Active'
            WHEN status = 2 THEN 'Pending'
            WHEN status = 3 THEN 'Rejected'
            WHEN status = 4 THEN 'Suspended'
            WHEN status = 5 THEN 'Disabled'
        END AS status,

        CASE
            WHEN status = 1 THEN NULL
            ELSE
                CONCAT(
                    UPPER(SUBSTRING(reject_reason, 1, 1)),
                    LOWER(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                SUBSTRING(reject_reason, 2), '([A-Z])', ' $1'
                            ),
                            '([0-9]+)', ' $1'
                        )
                    )
                )
        END AS reject_reason
    FROM
        {{ ref('scd2_mongo_product_state') }}
    WHERE
        dbt_valid_to IS NULL
),

certification AS (
    SELECT
        product_id,
        has_certification,
        certification_reason,
        has_registration,
        registration_reason
    FROM
        {{ ref('scd2_mongo_product_certification_states') }}
    WHERE
        dbt_valid_to IS NULL

),

merchant_products AS (
    SELECT DISTINCT merchant_id
    FROM {{ source('b2b_mart', 'merchant_products') }}
)

SELECT
    pp._id AS product_id,

    m.ali1688_product_id,
    m.joom_product_id,

    pa.m1688.categoryId AS ali1688_category_id,
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

    ps.status,
    ps.reject_reason,
    pp.dangerousKind AS dangerous_kind,

    c.has_certification,
    c.certification_reason,
    c.has_registration,
    c.registration_reason,

    pp.merchantId AS merchant_id,

    pp.origDescription AS orig_description,
    pp.origExtraImageUrls AS orig_extra_image_urls,
    pp.origMainImageUrl AS orig_main_image_url,
    pp.origName AS orig_name,
    pp.origUrl AS orig_url,

    pp.sku,
    pp.storeId AS store_id,

    IF(pp.merchantId = '66054380c33acc34a54a56d0', TRUE, FALSE) AS is_ali1688_product,
    CASE
        WHEN pp.merchantId = '66054380c33acc34a54a56d0' THEN 'external'
        WHEN mp.merchant_id IS NOT NULL THEN 'internal'
        ELSE 'other'
    END AS merchant_type,
    MILLIS_TO_TS_MSK(pp.updatedTimeMs) AS update_ts_msk,
    DATE(MILLIS_TO_TS_MSK(pp.updatedTimeMs)) AS update_date,
    MILLIS_TO_TS_MSK(pp.createdTimeMs) AS published_ts_msk,
    DATE(MILLIS_TO_TS_MSK(pp.createdTimeMs)) AS published_date
FROM {{ ref('scd2_published_products_snapshot') }} AS pp
LEFT JOIN {{ source('mongo', 'b2b_product_product_appendixes_daily_snapshot') }} AS pa ON pp._id = pa._id
LEFT JOIN product_states AS ps ON pp._id = ps.product_id
LEFT JOIN categories AS cat ON pp.categoryId = cat.category_id
LEFT JOIN matching AS m ON pp._id = m.product_id
LEFT JOIN certification AS c ON pp._id = c.product_id
LEFT JOIN merchant_products AS mp ON pp.merchantId = mp.merchant_id
WHERE pp.dbt_valid_to IS NULL
