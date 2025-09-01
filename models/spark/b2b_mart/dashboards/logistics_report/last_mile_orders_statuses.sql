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


WITH main AS (
    SELECT
        _id AS last_mile_order_id,
        sh.status AS status_id,
        sh.moderatorId AS moderator_id,
        sh.rejectReason AS reject_reason,
        sh.rejectReasonComment AS reject_reason_comment,
        MILLIS_TO_TS_MSK(sh.ctms) AS status_time,
        sh.switchAuto AS switch_auto
    FROM {{ source('mongo', 'b2b_core_issues_daily_snapshot') }}
    LATERAL VIEW EXPLODE(statusHistory) AS sh
    WHERE type = 36
),

ks AS (
    SELECT
        id,
        REPLACE(name, 'shipmentDealLastMile', '') AS name
    FROM {{ ref('key_status') }}
    WHERE key = 'issue.status'
),

joined AS (
    SELECT
        main.*,
        ks.name AS status_name,
        LEAD(ks.name) OVER (PARTITION BY last_mile_order_id ORDER BY status_time) AS lead_status_name,
        LEAD(status_time) OVER (PARTITION BY last_mile_order_id ORDER BY status_time) AS lead_status_time
    FROM main
    LEFT JOIN ks ON main.status_id = ks.id
)


SELECT
    last_mile_order_id,
    status_id,
    status_name,
    status_time,
    moderator_id,
    reject_reason,
    reject_reason_comment,
    switch_auto,
    lead_status_name,
    lead_status_time,
    lead_status_name IS NULL AS is_current_status
FROM joined
