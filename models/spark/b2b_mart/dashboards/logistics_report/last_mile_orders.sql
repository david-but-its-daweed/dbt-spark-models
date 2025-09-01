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
    _id AS last_mile_order_id,
    friendlyId AS last_mile_order_friendly_id,
    entityId AS shipment_deal_order_id,
    assigneeId AS assignee_id,
    assigneeHistory AS assignee_history,
    statusHistory AS status_history,
    MILLIS_TO_TS_MSK(ctms) AS created_time,
    MILLIS_TO_TS_MSK(utms) AS updated_time
FROM {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
WHERE type = 36
