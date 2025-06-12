{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH main AS (
    SELECT
        procurement_order_id,
        ps.status AS status,
        ps.subStatus AS sub_status,
        MILLIS_TO_TS_MSK(ps.statusTime) AS status_time
    FROM {{ ref('procurement_orders') }}
    LATERAL VIEW explode(procurement_statuses) AS ps
),

ks1 AS (
    SELECT
        id,
        name
    FROM {{ ref('key_status') }}
    WHERE key = 'orderproduct.procurementStatus'
),

ks2 AS (
    SELECT
        id,
        name,
        LPAD(CAST(ROW_NUMBER() OVER (ORDER BY id) AS STRING), 2, '0') AS sub_status_row_n
    FROM {{ ref('key_status') }}
    WHERE key = 'orderproduct.procurementSubStatus'
),

joined AS (
    SELECT
        ps.procurement_order_id,
        ps.status,
        ks1.name AS procurement_status_name,
        ps.sub_status,
        ks2.name AS procurement_sub_status_name,
        CONCAT(ks2.sub_status_row_n, '_', ks2.name) AS procurement_sub_status_name_row_n,
        ps.status_time,
        LEAD(ks2.name) OVER (PARTITION BY ps.procurement_order_id ORDER BY ps.status_time) AS lead_sub_status,
        LEAD(ps.status_time) OVER (PARTITION BY ps.procurement_order_id ORDER BY ps.status_time) AS lead_status_time
    FROM main AS ps
    LEFT JOIN ks1 ON ps.status = ks1.id
    LEFT JOIN ks2 ON ps.sub_status = ks2.id
)

SELECT
    *,
    CASE WHEN lead_sub_status = 'cancelled' THEN 1 ELSE 0 END AS is_next_status_cancelled,
    CAST((UNIX_TIMESTAMP(lead_status_time) - UNIX_TIMESTAMP(status_time)) / 3600 AS INT) AS lead_status_hours,
    CAST((UNIX_TIMESTAMP(lead_status_time) - UNIX_TIMESTAMP(status_time)) / 3600 / 24 AS INT) AS lead_status_days
FROM joined
ORDER BY
    procurement_order_id,
    status_time
