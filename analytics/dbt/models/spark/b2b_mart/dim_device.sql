{{ config(
    schema='b2b_mart',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_known_gaps': ['2022-12-19', '2022-12-20']
    }
) }}

WITH stg_devices as
(
    SELECT
        DATE(event_ts_msk) AS partition_date_msk,
        device_id,
        type,
        MAX(IF(TYPE="sessionStart",1,0)) OVER (PARTITION BY device_id, DATE(event_ts_msk)) as has_sessions,
        MAX(IF(TYPE="deviceCreate",1,0)) OVER (PARTITION BY device_id, DATE(event_ts_msk)) as has_device_create,
        FIRST(event_ts_msk) OVER (PARTITION BY device_id ORDER BY CASE WHEN TYPE="deviceCreate" THEN 1 ELSE 2 END, event_ts_msk)  AS join_ts,
        FIRST(device.osType) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN device.osType IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS os_type,
        FIRST(device.ipCountry) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN device.ipCountry IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS ip_country,
        FIRST(payload.link) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN payload.link IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS link,
        FIRST(payload.parsedUtm.utm_campaign) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_campaign IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_campaign,
        FIRST(payload.parsedUtm.utm_source) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_source IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_source,
        FIRST(payload.parsedUtm.utm_medium) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_medium IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_medium,
        FIRST(payload.parsedUtm.utm_term) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_term IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_term,
        FIRST(device.osVersion) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN device.osversion IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS os_version,
        FIRST(device.browserName) OVER (PARTITION BY device_id, DATE(event_ts_msk) ORDER BY CASE WHEN device.browserName IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS browser
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE 1=1
    {% if is_incremental() %}
      and DATE(event_ts_msk) >= date('{{ var("start_date_ymd") }}')
      and DATE(event_ts_msk) < date('{{ var("end_date_ymd") }}')
    {% else %}
      and DATE(event_ts_msk)  >= date('2022-05-01')
    {% endif %}
)


SELECT DISTINCT partition_date_msk,
       device_id,
       TIMESTAMP(join_ts) AS join_ts_msk,
       ip_country,
       os_type,
       link,
       utm_campaign,
       utm_source,
       utm_medium,
       utm_term,
       os_version,
	   browser
FROM stg_devices
WHERE has_sessions = 1
    AND has_device_create = 1





