{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH base AS (
    SELECT
        event_id,
        MILLIS_TO_TS_MSK(payload.timestamp) AS message_sent_ts_msk,
        payload.userId AS user_id,
        payload.contactId AS contact_id,
        payload.messageText AS message,
        payload.messageType AS message_type,
        payload.senderType AS sender_type,
        payload.sessionId AS thread_id,
        payload.status,
        payload.dealId AS deal_id,
        payload.stopReason AS stop_reason,
        payload.attachments
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE
        partition_date >= '2025-07-28'
        AND type = 'joomproChatMessageSent'
),

with_lag AS (
    SELECT
        *,
        LAG(sender_type) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk) AS prev_sender_type,
        (
            unix_timestamp(message_sent_ts_msk) -
            unix_timestamp(
                LAG(message_sent_ts_msk) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk)
            )
        ) / 3600 AS hours_since_prev
    FROM base
),

-- Обозначаем начало новой сессии:
    -- сообщение от клиента
    -- прошло 8+ часов с прошлого сообщения
    -- и между ними был наш ответ
sessionized AS (
    SELECT
        *,
        SUM(
            CASE
                WHEN sender_type = 'user'
                 AND hours_since_prev >= 8
                 AND prev_sender_type != 'user'
                THEN 1
                ELSE 0
            END
        ) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk) AS session_id
    FROM with_lag
)


SELECT
    event_id,
    contact_id,
    user_id,
    thread_id,
    session_id,
    message,
    message_sent_ts_msk,
    message_type,
    sender_type,
    status,
    deal_id,
    stop_reason,
    attachments
FROM sessionized
