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
    (DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) >= '2025-03-01' AND map.type = '2') AS is_internal,
    DATE(FROM_UNIXTIME(m.createdTimeMs / 1000)) AS created_date
FROM
    mongo.b2b_core_merchants_daily_snapshot AS m
LEFT JOIN
    mongo.b2b_core_merchant_appendixes_daily_snapshot AS map ON m._id = map._id
LEFT JOIN
    mongo.b2b_core_merchant_kyc_profiles_daily_snapshot AS kyc ON m._id = kyc._id