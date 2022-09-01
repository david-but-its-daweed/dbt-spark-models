{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

WITH popuprequest_event
(
    SELECT payload.userId AS user_id,
           payload.requestID AS request_id,
           payload.internalCustomer as is_stuff
    FROM {{ source('b2b_mart', 'device_events') }}
    WHERE type='popupRequestCreate'
        AND partition_date >= "2022-05-19"
)
SELECT _id AS request_id,
       TIMESTAMP(millis_to_ts_msk(ctms))  AS created_ts_msk,
       uid AS user_id,
       reason AS type,
       desc AS condition,
       map_from_entries(payload).`productId` AS product_id,
       COALESCE(map_from_entries(payload).`productLink`,map_from_entries(payload).`link`) AS link,
       map_from_entries(payload).`utm_medium` AS utm_medium,
       map_from_entries(payload).`utm_source` AS utm_source,
       map_from_entries(payload).`utm_campaign` AS utm_campaign,
       lower(tags[0])  AS tags,
       source,
       is_stuff AS is_joompro_employee
FROM {{ source('mongo', 'b2b_core_popup_requests_daily_snapshot') }} AS l
LEFT JOIN popuprequest_event AS p ON p.request_id=l._id
WHERE date(from_unixtime(floor(ctms / 1000), 'yyyy-MM-dd hh:mm:ss')) >= "2022-05-19"