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


WITH pulse_devices AS (
    SELECT DISTINCT
        user_id AS pulse_user_id,
        device_id
    FROM {{ ref("fact_user_activity") }}
),

pro_devices AS (
    SELECT DISTINCT
        de.user.userId AS pro_user_id,
        de.device_id
    FROM {{ source("b2b_mart", "device_events") }} AS de
    WHERE
        de.partition_date >= '2024-04-06'
        AND de.type IN ('sessionStart', 'bounceCheck')
        AND de.payload.pageUrl LIKE '%pt-br%'
        AND de.payload.pageUrl NOT LIKE '%gclid%'
),

pulse_phones AS (
    SELECT DISTINCT
        uid AS pulse_user_id,
        _id AS phone_number
    FROM {{ source("mongo", "b2b_core_analytics_phone_credentials_daily_snapshot") }}
),

pro_phones AS (
    SELECT DISTINCT
        uid AS pro_user_id,
        _id AS phone_number
    FROM {{ source("mongo", "b2b_core_phone_credentials_daily_snapshot") }}
)

SELECT
    pulse_user_id,
    pro_user_id,
    ARRAY_AGG(source) AS sources
FROM (
    SELECT
        pulse_devices.pulse_user_id,
        pro_devices.pro_user_id,
        'device' AS source
    FROM pulse_devices
    INNER JOIN pro_devices ON pulse_devices.device_id = pro_devices.device_id
    UNION ALL
    SELECT
        pulse_phones.pulse_user_id,
        pro_phones.pro_user_id,
        'phone' AS source
    FROM pulse_phones
    INNER JOIN pro_phones ON pulse_phones.phone_number = pro_phones.phone_number
)
GROUP BY
    pulse_user_id,
    pro_user_id