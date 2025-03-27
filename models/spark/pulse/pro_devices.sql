{{ config(
    schema='joompro_analytics_internal_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'bigquery_project_id': 'joom-analytics-joompro-public',
      'bigquery_check_counts': 'false',
      'priority_weight': '150'
    }
) }}

WITH phone_numbers AS (
    SELECT DISTINCT
        uid AS user_id,
        _id AS phone_number
    FROM {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
)

SELECT DISTINCT
    de.user.userId AS user_id,
    de.device_id,
    pn.phone_number
FROM {{ source("b2b_mart", "device_events") }} AS de
LEFT JOIN phone_numbers AS pn ON pn.user_id = de.user.userId
WHERE
    de.partition_date >= '2024-04-06'
    AND de.type IN ('sessionStart', 'bounceCheck')
    AND de.payload.pageUrl LIKE '%pt-br%'
    AND de.payload.pageUrl NOT LIKE '%gclid%'
