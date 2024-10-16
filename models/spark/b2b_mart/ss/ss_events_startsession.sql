
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
    utm_labels as (
    SELECT DISTINCT
        first_value(labels.utm_source) over (partition by user_id order by event_ts_msk) as utm_source,
        first_value(labels.utm_medium) over (partition by user_id order by event_ts_msk) as utm_medium,
        first_value(labels.utm_campaign) over (partition by user_id order by event_ts_msk) as utm_campaign,
        first_value(labels.gad_source) over (partition by user_id order by event_ts_msk) as gad_source,
        user_id
    from
      (
    select
    map_from_arrays(transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> case when split_part(x, '=', 1) != "gclid" then split_part(x, '=', 1) else "gclid" || uuid() end
        ),
        
        transform(
            split(split_part(payload.pageUrl, "?", -1), '&'), 
            x -> split_part(x, '=', 2)
        )
        ) as labels,
        user.userId as user_id, event_ts_msk
    from {{ source('b2b_mart', 'device_events') }}
    where type = 'sessionStart' and and  payload.pageUrl not like  '%https://joompro.ru/ru%' and ( payload.pageUrl like "%utm%" 
                                        or payload.pageUrl like "%gad_source%" 
                                        or payload.pageUrl like "%gclid%" )
    )
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
LEFT JOIN utm_labels  ON  de.user['userId'] = utm_labels.user_id 
left join bots on bots.device_id = de.device.id
WHERE
  partition_date >= '2024-04-06'
  AND type IN ('sessionStart',
    'bounceCheck')
