{{
  config(
    materialized='table',
    partition_by={
      "field": "day",
    }
  )
}}

WITH babylone_ticket_create_joom_100 AS (
    SELECT
        id,
        partition_date,
        event_ts_utc,
        payload.customerExternalId AS customer_external_id,
        payload.ticketId AS ticket_id,
        payload.lang,
        payload.messageSource AS message_source
    FROM {{ source("mart", "babylone_events") }}
    WHERE `type` = 'ticketCreateJoom'
),

tickets AS (
    SELECT
        e.id,
        e.partition_date AS day,
        CAST(e.event_ts_utc AS TIMESTAMP) AS event_ts,
        e.customer_external_id AS user_id,
        e.ticket_id,
        ol.element AS order_id,
        e.lang,
        e.message_source,
    FROM babylone_ticket_create_joom_100 as e
    LEFT JOIN UNNEST(order_ids.list) ol
),

active_users AS (
    SELECT *
    FROM {{ ref('active_users') }}
    WHERE day >= '2020-09-01'
),

tickets_ext AS (
    SELECT 
        a.*, 
        b.platform,
        b.country, 
    FROM tickets a
    LEFT JOIN active_users b ON a.day = b.day AND a.user_id = b.user_id
)

SELECT * FROM tickets_ext