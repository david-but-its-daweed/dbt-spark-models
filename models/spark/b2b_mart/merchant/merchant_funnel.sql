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
    SELECT m.*
    FROM {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }} AS m
    LEFT JOIN {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }} AS ap ON m._id = ap._id
    WHERE
        DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) >= '2025-03-01' AND ap.type = '2'
        OR
        m._id IN (
            '61cc1882f567381cb20338bf',
            '6206891d23e71a0032dbc9a0',
            '6743eabfd76f37ab8841eafb',
            '623c0e58222542d6f1236447',
            '676a4d2beeb675d30389d043',
            '64254b3135d4115da48679a0',
            '6850052109a0bf1534da972b',
            '684c0518701dadd2e101dbee',
            '68479f3d1dcc89034a7fa7e0'
        )
),

uploaded_products AS (
    SELECT
        merchant_id,
        MIN(published_date) AS first_product_uploaded_date
    FROM
        {{ source('b2b_mart', 'ss_assortment_products') }}
    WHERE
        status = 'Active'
    GROUP BY
        1
),

previews AS (
    SELECT
        FROM_JSON(e.event_params, 'product_id STRING').product_id AS product_id,
        TO_DATE(MIN(e.event_time)) AS first_product_seen_date
    FROM
        {{ source('b2b_mart', 'ss_events_by_session') }}
        LATERAL VIEW EXPLODE(events_in_session) AS e
    WHERE
        e.event_type = 'productPreview'
    GROUP BY
        1
),

previews_by_merchants AS (
    SELECT
        ap.merchant_id,
        MIN(p.first_product_seen_date) AS first_product_seen_date
    FROM
        previews AS p
    INNER JOIN
        {{ source('b2b_mart', 'ss_assortment_products') }} AS ap ON p.product_id = ap.product_id
    GROUP BY
        1
),

deals AS (
    SELECT
        ap.merchant_id,
        MIN(DATE(cr.created_time)) AS first_deal_created_date
    FROM
        {{ source('b2b_mart', 'fact_customer_requests') }} AS cr
    LEFT JOIN
        {{ source('b2b_mart', 'ss_assortment_products') }} AS ap ON cr.product_id = ap.product_id
    WHERE
        cr.country = 'BR'
        AND cr.next_effective_ts_msk IS NULL
    GROUP BY
        1
)

SELECT
    m._id AS merchant_id,
    m.name AS merchant_name,
    m.enabled AS is_enabled,

    CASE
        WHEN kyc.status = 0 THEN 'Action required'
        WHEN kyc.status = 10 THEN 'On review'
        WHEN kyc.status = 20 THEN 'Passed'
        WHEN kyc.status = 30 THEN 'Failed'
        ELSE 'No status'
    END AS kyc_status,

    DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) AS created_date,
    CASE WHEN kyc.status = 20 THEN DATE(kyc.updatedtime) END AS kyc_approved_date,
    up.first_product_uploaded_date,
    pbm.first_product_seen_date,
    d.first_deal_created_date
FROM
    merchants AS m
LEFT JOIN
    mongo.b2b_core_merchant_kyc_profiles_daily_snapshot AS kyc ON m._id = kyc._id
LEFT JOIN
    uploaded_products AS up ON m._id = up.merchant_id
LEFT JOIN
    previews_by_merchants AS pbm ON m._id = pbm.merchant_id
LEFT JOIN
    deals AS d ON m._id = d.merchant_id