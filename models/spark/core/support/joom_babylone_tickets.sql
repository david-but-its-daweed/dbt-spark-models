{{
  config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='delta',
    partition_by=['day'],
  )
}}


WITH babylone_ticket_create_joom_100 AS (
    SELECT
        id,
        partition_date,
        event_ts_utc,
        payload.customerexternalid AS customer_external_id,
        payload.ticketid AS ticket_id,
        payload.lang,
        payload.messagesource AS message_source,
        payload.orderids AS order_ids
    FROM {{ source("mart", "babylone_events") }}
    WHERE type = 'ticketCreateJoom'
    {% if is_incremental() %}
        AND partition_date >= DATE '{{ var("start_date_ymd") }}'
        AND partition_date < DATE '{{ var("end_date_ymd") }}'
    {% elif target.name != 'prod' %}
        AND partition_date >= date_sub(current_date(), 7)
        AND partition_date < current_date()
    {% endif %}
),

tickets AS (
    SELECT
        e.id,
        e.partition_date AS day,
        CAST(e.event_ts_utc AS TIMESTAMP) AS event_ts,
        e.customer_external_id AS user_id,
        e.ticket_id,
        order_id,
        e.lang,
        e.message_source
    FROM
        babylone_ticket_create_joom_100 AS e
            LATERAL VIEW OUTER EXPLODE(order_ids) ol AS order_id
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
        b.country
    FROM tickets AS a
    LEFT JOIN active_users AS b ON a.day = b.day AND a.user_id = b.user_id
)

SELECT * FROM tickets_ext