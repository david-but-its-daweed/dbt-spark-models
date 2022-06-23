{{ config(
    schema='example',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

    SELECT
        DATE(event_ts_msk) AS partition_date_msk,
        device_id,
        user.userId AS user_id,
        payload.requestID AS request_id,
        MIN(device.osType) OVER (PARTITION BY device_id, user.userId,  DATE(event_ts_msk))  AS event_ts_min_msk,
        MAX(device.osType) OVER (PARTITION BY device_id, user.userId,  DATE(event_ts_msk))  AS event_ts_max_msk,
        COUNT(IF(TYPE = "sessionStart",1,0))  OVER (PARTITION BY device_id, user.userId)  AS session_start_count,
        FIRST(device.osType) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN device.osType IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS os_type,
        FIRST(device.ipCountry) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN device.ipCountry IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS ip_country,
        FIRST(payload.link) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN payload.link IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS link,
        FIRST(payload.parsedUtm.utm_campaign) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_campaign IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_campaign,
        FIRST(payload.parsedUtm.utm_source) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_source IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_source,
        FIRST(payload.parsedUtm.utm_medium) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_medium IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_medium,
        FIRST(payload.parsedUtm.utm_term) OVER (PARTITION BY device_id, user.userId, DATE(event_ts_msk) ORDER BY CASE WHEN payload.parsedUtm.utm_term IS NOT NULL THEN 1 ELSE 2 END, event_ts_msk)  AS utm_term
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE `type` in ("sessionStart","externalLink","bounceCheck","popupRequestCreate")
{% if is_incremental() %}
  and DATE(event_ts_msk) >= date'{{ var("start_date_ymd") }}'
  and DATE(event_ts_msk) < date'{{ var("end_date_ymd") }}'
{% else %}
  and partition_date >= date'2022-05-01'
{% endif %}

