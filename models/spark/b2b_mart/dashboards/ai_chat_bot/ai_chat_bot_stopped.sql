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
    MILLIS_TO_TS_MSK(payload.timestamp) AS bot_stopped_ts_msk,
    payload.userId AS user_id,
    payload.contactId AS contact_id,
    payload.lastBotMessage AS last_bot_message,
    payload.previousMessage AS previous_message,
    payload.previousUserMessage AS previous_user_message,
    payload.userMessage AS user_message,
    payload.stopReason AS stop_reason
FROM {{ source('b2b_mart', 'operational_events') }}
WHERE partition_date >= '2025-07-28'
  AND type = 'joomproChatbotStopped'
