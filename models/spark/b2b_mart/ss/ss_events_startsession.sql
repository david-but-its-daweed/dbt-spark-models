
{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}



WITH
  users AS (
  SELECT
    user['userId'] AS user_id,
    CAST(MIN(event_ts_msk) AS DATE) AS valid_msk_date,
    MAX(1) AS active_user
  FROM
     {{ source('b2b_mart', 'device_events') }}
  WHERE
    partition_date >= '2024-04-06'
  GROUP BY
    1
  HAVING
    SUM(
    IF
      (type IN (
          "productPreview",
          "searchOpen",
          "categoryClick",
          "orderPreview",
          "productGalleryScroll",
          "categoryOpen",
          "popupFormSubmit",
          "popupFormNext",
          "mainClick",
          "orderClick"), 1, 0)) > 0 )
SELECT
  user['userId'] AS user_id,
  type,
  event_ts_utc,
  CAST(event_ts_utc AS DATE) AS event_utc_date,
  event_ts_msk,
  CAST(event_ts_msk AS DATE) AS event_msk_date,
  payload.pageUrl AS pageUrl,
  device.osType AS osType,
  device.osVersion AS osVersion,
  device.browserName AS browserName,
  COALESCE(active_user,0) AS active_user,
  regexp_extract(payload.pageUrl, 'https://joom.pro/([^/?]+)', 1) AS landing
FROM
   {{ source('b2b_mart', 'device_events') }} AS de
LEFT JOIN
  users
ON
  de.user['userId'] = users.user_id
  AND CAST(de.event_ts_msk AS DATE) >= users.valid_msk_date
WHERE
  partition_date >= '2024-04-06'
  AND type IN ('sessionStart',
    'bounceCheck')
