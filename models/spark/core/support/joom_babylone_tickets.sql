{{
  config(
    materialized='table',
    partition_by={
      "field": "day",
    }
  )
}}

WITH tickets as 
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
  FROM {{source('sample_events', 'babylone_ticket_create_joom_100')}}
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