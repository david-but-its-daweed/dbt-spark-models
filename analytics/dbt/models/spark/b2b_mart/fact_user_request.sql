{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}
SELECT _id AS lead_id,
       from_unixtime(floor(ctms / 1000), 'yyyy-MM-dd hh:mm:ss')  AS created_ts_utc,
       uid AS user_id,
       reason AS type,
       desc AS condition,
       map_from_entries(payload).`productId` AS product_id,
       COALESCE(map_from_entries(payload).`productLink`,map_from_entries(payload).`link`) AS link,
       map_from_entries(payload).`utm_medium` AS utm_medium,
       map_from_entries(payload).`utm_source` AS utm_source,
       map_from_entries(payload).`utm_campaign` AS utm_campaign,
       lower(tags[0])  AS tags
FROM {{ source('mongo', 'b2b_core_popup_requests_daily_snapshot') }} AS l
--LEFT JOIN popuprequest_event AS p ON l.
WHERE date(from_unixtime(floor(ctms / 1000), 'yyyy-MM-dd hh:mm:ss')) >= "2022-05-19"