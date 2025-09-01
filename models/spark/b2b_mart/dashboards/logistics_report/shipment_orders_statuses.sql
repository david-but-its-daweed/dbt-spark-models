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
        _id AS shipment_order_id,
        sh.status AS status_id,
        CASE sh.status
            WHEN 5 THEN 'Created'
            WHEN 12 THEN 'Ready For Pickup'
            WHEN 20 THEN 'Shipped'
            WHEN 35 THEN 'Cancelled'
        END AS status_name,
        MILLIS_TO_TS_MSK(sh.updatedTimeMs) AS status_time
    FROM {{ source('mongo', 'b2b_core_shipment_orders_daily_snapshot') }}
        LATERAL VIEW EXPLODE(statusHistory) AS sh
),

m AS (
    SELECT
        *,
        LEAD(status_name) OVER (PARTITION BY shipment_order_id ORDER BY status_time) AS lead_status_name,
        LEAD(status_time) OVER (PARTITION BY shipment_order_id ORDER BY status_time) AS lead_status_time
    FROM main
)


SELECT
    *,
    lead_status_name IS NULL AS is_current_status
FROM m
