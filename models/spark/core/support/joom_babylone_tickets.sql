{{
  config(
    materialized='table',
    partition_by={
      "field": "day",
    }
  )
}}

WITH babylone_ticket_create_joom_100 as
(
  SELECT
    id,
    partition_date,
    event_ts_utc,
    payload.customerExternalId as customer_external_id,
    payload.ticketId as ticket_id,
    payload.lang,
    payload.messageSource as message_source
  from mart.babylone_events
  where type= 'ticketCreateJoom'
),
tickets as
(
  SELECT 
    id,
    partition_date  as day,
    CAST(event_ts_utc as DATETIME) as event_ts, 
    customer_external_id as user_id,
    ticket_id,
    ol.element as order_id,
    lang,
    message_source,
  FROM babylone_ticket_create_joom_100
  LEFT JOIN UNNEST(order_ids.list) ol
), 
tickets_ext as (
    SELECT 
        a.*, 
        b.platform,
        b.country, 
    FROM tickets a
    LEFT JOIN (SELECT * FROM {{ ref('active_users')}} WHERE day >= '2020-09-01') b ON a.day = b.day AND a.user_id = b.user_id
)

SELECT * FROM tickets_ext