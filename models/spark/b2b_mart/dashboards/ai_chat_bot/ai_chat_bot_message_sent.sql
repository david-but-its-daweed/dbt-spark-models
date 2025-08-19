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
        payload.attachments,
        -- Убираем дубли
        ROW_NUMBER() OVER (PARTITION BY payload.contactId, payload.timestamp, payload.messageText ORDER BY event_id) AS row_n
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
            UNIX_TIMESTAMP(message_sent_ts_msk)
            - UNIX_TIMESTAMP(
                LAG(message_sent_ts_msk) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk)
            )
        ) / 3600 AS hours_since_prev
    FROM base
    WHERE row_n = 1
),

-- Обозначаем начало новой сессии:
-- сообщение от клиента
-- прошло 8+ часов с прошлого сообщения
-- и между ними был наш ответ
sessionized AS (
    SELECT
        *,
        CONCAT(
            contact_id, '_', SUM(
                CASE
                    WHEN
                        sender_type = 'user'
                        AND hours_since_prev >= 8
                        AND prev_sender_type != 'user'
                        THEN 1
                    ELSE 0
                END
            ) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk)
        ) AS session_id
    FROM with_lag
),

windowed AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY message_sent_ts_msk) AS row_n_in_session,
        ROW_NUMBER() OVER (PARTITION BY session_id, sender_type ORDER BY message_sent_ts_msk) AS row_n_in_session_by_sender,
        CASE
            WHEN
                LAG(sender_type) OVER (PARTITION BY session_id ORDER BY message_sent_ts_msk) IS NULL
                OR LAG(sender_type) OVER (PARTITION BY session_id ORDER BY message_sent_ts_msk) != sender_type
                THEN 1
            ELSE 0
        END AS start_of_block
    FROM sessionized
),

with_blocks AS (
    SELECT
        *,
        -- глобальный порядковый номер блока в сессии
        SUM(start_of_block) OVER (
            PARTITION BY session_id ORDER BY message_sent_ts_msk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id,
        -- порядковый номер блока ДЛЯ КОНКРЕТНОГО sender_type в рамках сессии
        SUM(
            CASE WHEN start_of_block = 1 THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY session_id, sender_type ORDER BY message_sent_ts_msk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id_by_sender
    FROM windowed
),

-- Порядковый номер сообщения внутри блока (удобно для выборки начала/конца блока)
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id, block_id ORDER BY message_sent_ts_msk) AS block_row_n
    FROM with_blocks
)


SELECT
    event_id,
    contact_id,
    user_id,
    thread_id,
    session_id,
    block_id,
    block_id_by_sender,
    block_row_n,
    sender_type,
    message_sent_ts_msk,
    message,
    message_type,
    status,
    deal_id,
    stop_reason,
    attachments,
    row_n_in_session,
    row_n_in_session_by_sender
FROM final
