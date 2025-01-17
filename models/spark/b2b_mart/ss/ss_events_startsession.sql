
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
    bots as (
                            select device_id, max(1) as bot_flag
                        from threat.bot_devices_joompro 
                        where is_device_marked_as_bot or is_retrospectively_detected_bot
                        group by 1 ) ,
    extracted_params AS (
    SELECT
        id,
        user.userId AS user_id,
        event_ts_msk,
        transform(
            split(split_part(payload.pageUrl, "?", -1), '&'),
            x -> struct(
                split_part(x, '=', 1) AS key,
                split_part(x, '=', 2) AS value
            )
        ) AS params
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE 
        type = 'sessionStart'
        AND payload.pageUrl NOT LIKE '%https://joompro.ru/ru%'
        AND (
            payload.pageUrl LIKE '%utm%'
            OR payload.pageUrl LIKE '%gad_source%'
            OR payload.pageUrl LIKE '%gclid%'
        )
       AND partition_date >= '2024-04-06'
),
flattened_params AS (
    SELECT
        id,
        user_id,
        event_ts_msk,
        key,
        concat_ws('_tech_merged_', collect_set(value)) AS merged_values
    FROM (
        SELECT
            id,
            user_id,
            event_ts_msk,
            inline(params) AS (key, value)
        FROM extracted_params
    )
    GROUP BY id, user_id, event_ts_msk, key
),
utm_labels AS (
    SELECT
        id,
        user_id,
        event_ts_msk,
        MAX(CASE WHEN key = 'utm_source' THEN merged_values ELSE NULL END) AS utm_source,
        MAX(CASE WHEN key = 'utm_medium' THEN merged_values ELSE NULL END) AS utm_medium,
        MAX(CASE WHEN key = 'utm_campaign' THEN merged_values ELSE NULL END) AS utm_campaign,
        MAX(CASE WHEN key = 'gad_source' THEN merged_values ELSE NULL END) AS gad_source
    FROM flattened_params
    GROUP BY id, user_id, event_ts_msk
),
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
  de.device.id as device_id,
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
  regexp_extract(payload.pageUrl, 'https://joom.pro/([^/?]+)', 1) AS landing,
  case when utm_source is Null and gad_source is not null then 'unrecognized_google_advertising' else utm_source end as utm_source ,
  utm_medium,
  utm_campaign,
  coalesce(bot_flag,0) as bot_flag
FROM
   {{ source('b2b_mart', 'device_events') }} AS de
LEFT JOIN
  users
ON
  de.user['userId'] = users.user_id
  AND CAST(de.event_ts_msk AS DATE) >= users.valid_msk_date
LEFT JOIN utm_labels  ON de.id = utm_labels.id   --- de.user['userId'] = utm_labels.user_id 
left join bots on bots.device_id = de.device.id
WHERE
  partition_date >= '2024-04-06'
  AND type IN ('sessionStart',
    'bounceCheck')
