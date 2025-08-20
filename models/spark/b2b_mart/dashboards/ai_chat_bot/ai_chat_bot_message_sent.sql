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
        ROW_NUMBER() OVER (PARTITION BY payload.contactId, payload.timestamp, payload.messageText ORDER BY event_id) AS row_n
    FROM {{ source('b2b_mart', 'operational_events') }}
    WHERE
        partition_date >= '2025-07-28'
        AND type = 'joomproChatMessageSent'
),

-- Убираем дубли
base_clean AS (
    SELECT *
    FROM base
    WHERE row_n = 1
),

-- Считаем разрыв в часах между сообщениями клиента
user_messages AS (
    SELECT
        *,
        (UNIX_TIMESTAMP(message_sent_ts_msk) - UNIX_TIMESTAMP(prev_message_ts)) / 3600 AS hours_since_prev
    FROM (
        SELECT
            *,
            LAG(message_sent_ts_msk) OVER (PARTITION BY contact_id ORDER BY message_sent_ts_msk) AS prev_message_ts
        FROM base_clean
        WHERE sender_type = 'user'
    )
),

-- Определяем номер пользовательской сессии:
-- новая сессия начинается, если это первое сообщение клиента или разрыв между его сообщениями >= 8 часов
session_by_user AS (
    SELECT
        *,
        SUM(
            CASE WHEN (hours_since_prev IS NULL OR hours_since_prev >= 8) THEN 1 ELSE 0 END
        ) OVER (
            PARTITION BY contact_id
            ORDER BY message_sent_ts_msk
        ) AS session_num
    FROM user_messages
),

-- Протягиваем session_num клиента на все сообщения до следующего старта сессии
sessionized AS (
    SELECT
        t1.*,
        LAST_VALUE(t2.session_num) IGNORE NULLS OVER (
            PARTITION BY t1.contact_id
            ORDER BY t1.message_sent_ts_msk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS session_num_ffil
    FROM base_clean AS t1
    LEFT JOIN session_by_user AS t2 ON t1.event_id = t2.event_id
),

-- Формируем session_id.
-- Если сессии ещё не было (сообщения только от нас до первого сообщения клиента), ставим _our_first_messages
with_session_id AS (
    SELECT
        *,
        CASE
            WHEN session_num_ffil IS NOT NULL
                THEN CONCAT(contact_id, '_', CAST(session_num_ffil AS STRING))
            ELSE CONCAT(contact_id, '_our_first_messages')
        END AS session_id
    FROM sessionized
),

-- считаем порядковые номера сообщений и определяем границы "блоков":
-- блок = последовательность сообщений от одного отправителя без переключения
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
    FROM with_session_id
),

-- считаем идентификаторы блоков
with_blocks AS (
    SELECT
        *,
        -- глобальный порядковый номер блока в сессии
        SUM(start_of_block) OVER (
            PARTITION BY session_id ORDER BY message_sent_ts_msk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id,
        -- порядковый номер блока ДЛЯ КОНКРЕТНОГО sender_type (например: "вторая подсессия бота")
        SUM(CASE WHEN start_of_block = 1 THEN 1 ELSE 0 END) OVER (
            PARTITION BY session_id, sender_type ORDER BY message_sent_ts_msk
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS block_id_by_sender
    FROM windowed
),

-- порядковый номер сообщения внутри блока
final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY session_id, block_id ORDER BY message_sent_ts_msk) AS block_message_row_n
    FROM with_blocks
)


SELECT
    event_id,
    contact_id,
    user_id,
    thread_id,
    session_id,                 -- ID сессии (якорь = сообщения клиента)
    block_id,                   -- глобальный номер блока в сессии
    block_id_by_sender,         -- номер блока для конкретного отправителя
    block_message_row_n,        -- порядковый номер сообщения внутри блока
    sender_type,
    message_sent_ts_msk,
    message,
    message_type,
    status,
    deal_id,
    stop_reason,
    attachments,
    row_n_in_session,           -- порядковый номер сообщения в сессии
    row_n_in_session_by_sender  -- порядковый номер сообщения отправителя в сессии
FROM final
ORDER BY message_sent_ts_msk
