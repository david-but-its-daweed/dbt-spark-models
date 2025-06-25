{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

WITH merchants AS (
    SELECT
        m._id AS merchant_id,
        m.name,
        m.email,
        m.Enabled,
        CASE
            WHEN kyc.status = 0 THEN 'Action required'
            WHEN kyc.status = 10 THEN 'On review'
            WHEN kyc.status = 20 THEN 'Passed'
            WHEN kyc.status = 30 THEN 'Failed'
            ELSE 'No status'
        END AS kyc_status,
        DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) AS created_date,
        ap.type AS merchant_type
    FROM
        {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }} AS m
    LEFT JOIN
        {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }} AS ap ON m._id = ap._id
    LEFT JOIN
        {{ source('mongo', 'b2b_core_merchant_kyc_profiles_daily_snapshot') }} AS kyc ON m._id = kyc._id
),

internal_merchants AS (
    SELECT *
    FROM merchants
    WHERE
        (created_date >= '2025-03-01' AND merchant_type = 2)
        OR
        merchant_id IN (
            '676a4d2beeb675d30389d043',
            '64254b3135d4115da48679a0',
            '676a4d2beeb675d30389d043',
            '6850052109a0bf1534da972b',
            '684c0518701dadd2e101dbee',
            '68479f3d1dcc89034a7fa7e0'
        )
),

categories AS (
    SELECT
        category_id,
        name AS category_name,
        level_1_category.name AS level_1_category_name,
        level_2_category.name AS level_2_category_name,
        level_3_category.name AS level_3_category_name,
        level_1_category.id AS level_1_category_id,
        level_2_category.id AS level_2_category_id,
        level_3_category.id AS level_3_category_id
    FROM
        {{ source('mart', 'category_levels') }}
),

products AS (
    SELECT DISTINCT
        product_id,
        merchant_id,
        category_id,
        orig_name,
        orig_main_image_url,
        created_date
    FROM
        {{ ref('ss_assortment_products') }}
),

product_prices AS (
    SELECT
        v.pId AS product_id,
        p.ccy AS currency,
        MIN(ELEMENT_AT(v.prc, -1)) / 1e6 AS min_price,
        MAX(ELEMENT_AT(v.prc, 1)) / 1e6 AS max_price
    FROM
        {{ source('mongo', 'b2b_product_variant_appendixes_daily_snapshot') }} AS v
    LEFT JOIN
        {{ source('mongo', 'b2b_product_product_appendixes_daily_snapshot') }} AS p
        ON v.pId = p._id
    WHERE
        p.ccy IS NOT NULL
    GROUP BY
        1, 2
),

product_states AS (
    SELECT
        product_id,
        update_ts_msk,
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

variants AS (
    SELECT
        product_id,
        COUNT(DISTINCT variant_id) AS variant_cnt
    FROM
        {{ ref('sat_published_variant') }}
    GROUP BY
        1
),

result AS (
    SELECT
        im.merchant_id,
        im.name AS merchant_name,

        im.Enabled AS merchant_enabled,
        im.kyc_status,

        im.email,
        im.created_date AS merchant_created_date,
        CONCAT('https://admin.joompro.io/merchants/', im.merchant_id) AS merchant_link,

        p.product_id,
        p.orig_name AS product_name,
        p.orig_main_image_url AS product_image_url,

        ps.status AS product_status,
        ps.reject_reason AS product_reject_reason,

        p.category_id,
        c.level_1_category_name AS l1_category_name,
        c.level_2_category_name AS l2_category_name,
        c.level_3_category_name AS l3_category_name,
        p.created_date AS product_created_date,
        DATE(ps.update_ts_msk) AS product_last_updated_date,
        CONCAT('https://admin.joompro.io/products/', p.product_id) AS product_link,

        pp.min_price,
        pp.max_price,
        pp.currency,

        v.variant_cnt
    FROM
        internal_merchants AS im
    LEFT JOIN
        products AS p ON im.merchant_id = p.merchant_id
    LEFT JOIN
        categories AS c ON p.category_id = c.category_id
    LEFT JOIN
        product_states AS ps ON p.product_id = ps.product_id
    LEFT JOIN
        product_prices AS pp ON p.product_id = pp.product_id
    LEFT JOIN
        variants AS v ON p.product_id = v.product_id
)

SELECT *
FROM result
WHERE product_id IS NOT NULL