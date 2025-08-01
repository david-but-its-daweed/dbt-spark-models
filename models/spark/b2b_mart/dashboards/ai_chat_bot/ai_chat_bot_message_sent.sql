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


SELECT
    event_id,
    MILLIS_TO_TS_MSK(payload.timestamp) AS message_sent_ts_msk,
    payload.userId AS user_id,
    payload.contactId AS contact_id,
    payload.messageText AS message,
    payload.messageType AS message_type,
    payload.senderType AS sender_type,
    payload.sessionId AS session_id,
    payload.status,
    payload.dealId AS deal_id,
    payload.stopReason AS stop_reason,
    payload.attachments
FROM {{ source('b2b_mart', 'operational_events') }}
WHERE
    partition_date >= '2025-07-28'
    AND type = 'joomproChatMessageSent'
