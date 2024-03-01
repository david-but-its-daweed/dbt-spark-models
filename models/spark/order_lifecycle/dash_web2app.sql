{{ config(
    schema='product',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['visit_date_msk'],
    meta = {
      'model_owner' : '@ilypavlov',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'visit_date_msk',
      'bigquery_fail_on_missing_partitions': 'false',
      'priority_weight': '150'
    }
) }}

WITH web_attr AS (
    SELECT
        device_id,
        MAX(source) AS partner
    FROM {{ source('ads', 'web_analytics_pageviews_with_spend') }}
    WHERE partition_date >= '2023-01-01'
    GROUP BY 1
),

active_web2app AS (
    SELECT
        t.device_id,
        t.payload,
        t.partition_date,
        t.device_from_event.version.`osType` AS os_type,
        t.`type`,
        a.partner
    FROM {{ source('mart', 'device_events') }} AS t
    LEFT JOIN web_attr AS a
        ON t.device_id = a.device_id
    WHERE
        t.`type` = 'externalLink'
        AND t.partition_date >= '2023-01-01'
),

web_client_transitions AS (
    SELECT DISTINCT
        payload.params['utm_device_id'] AS web_device_id,
        device_id AS client_device_id,
        LOWER(os_type) AS os_type,
        partition_date AS date_msk,
        partner
    FROM active_web2app
    WHERE
        type = 'externalLink'
        AND payload.params['utm_device_id'] IS NOT NULL
        AND device_id != payload.params['utm_device_id']
        AND LOWER(os_type) IN ('ios', 'android')
),

min_purchase AS (
    SELECT
        t.client_device_id,
        MIN(a.order_date_msk) AS order_date
    FROM web_client_transitions AS t
    INNER JOIN {{ ref('gold_orders') }} AS a
        ON
            t.client_device_id = a.device_id
            AND a.order_date_msk >= '2023-01-01'
    GROUP BY 1
),

exp_devices AS (
    SELECT DISTINCT
        t.device_id,
        t.date_msk,
        t.platform,
        t.join_date_msk AS join_date
    FROM {{ ref('gold_active_devices') }} AS t
    WHERE
        t.platform IN ('trampolineweb', 'fullweb')
        AND t.date_msk >= '2023-01-01'
)

SELECT
    exp_devices.date_msk AS visit_date_msk,
    web_client_transitions.partner AS partner,
    exp_devices.platform AS platform,
    CASE WHEN exp_devices.join_date = exp_devices.date_msk THEN 1 ELSE 0 END AS is_new_device,
    CASE WHEN DATEDIFF(a.order_date, web_client_transitions.date_msk) <= 7 THEN 1 ELSE 0 END AS is_cr_app_week,
    CASE WHEN DATEDIFF(a.order_date, web_client_transitions.date_msk) <= 30 THEN 1 ELSE 0 END AS is_cr_app_month,
    COUNT(DISTINCT web_client_transitions.client_device_id) AS devices_visit_app,
    COUNT(DISTINCT exp_devices.device_id) AS devices_visit_web
FROM exp_devices
LEFT OUTER JOIN web_client_transitions
    ON
        exp_devices.device_id = web_client_transitions.web_device_id
        AND exp_devices.date_msk = web_client_transitions.date_msk
LEFT OUTER JOIN min_purchase AS a
    ON web_client_transitions.client_device_id = a.client_device_id
GROUP BY 1, 2, 3, 4, 5, 6
ORDER BY 1, 2, 3, 4, 5, 6
