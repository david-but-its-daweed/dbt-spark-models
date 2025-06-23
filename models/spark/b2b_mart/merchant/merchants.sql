{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@daweed',
      'bigquery_load': 'true',
    }
) }}

SELECT
    m._id AS merchant_id,
    m.name AS merchant_name,
    m.companyname AS company_name,
    m.email,

    CASE
        WHEN kyc.status = 0 THEN 'Action required'
        WHEN kyc.status = 10 THEN 'On review'
        WHEN kyc.status = 20 THEN 'Passed'
        WHEN kyc.status = 30 THEN 'Failed'
        ELSE 'No status'
    END AS kyc_status,

    m.enabled AS is_enabled,
    (
        DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) >= '2025-03-01' AND ap.type = '2'
        OR
        m._id IN (
            '676a4d2beeb675d30389d043',
            '64254b3135d4115da48679a0',
            '676a4d2beeb675d30389d043',
            '6850052109a0bf1534da972b',
            '684c0518701dadd2e101dbee',
            '68479f3d1dcc89034a7fa7e0'
        )
    ) AS is_internal,
    DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) AS created_date
FROM
    {{ source('mongo', 'b2b_core_merchants_daily_snapshot') }} AS m
LEFT JOIN
    {{ source('mongo', 'b2b_core_merchant_appendixes_daily_snapshot') }} AS ap ON m._id = ap._id
LEFT JOIN
    {{ source('mongo', 'b2b_core_merchant_kyc_profiles_daily_snapshot') }} AS kyc ON m._id = kyc._id