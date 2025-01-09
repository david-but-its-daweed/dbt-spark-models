{{
  config(
    materialized='incremental',
    alias='fbj_merchant_replenishments',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    },
    on_schema_change='append_new_columns'
  )
}}

WITH status_raw AS (
    SELECT
        EXPLODE(c.sh) AS statusHistory,
        c._id
    FROM
        {{ source('mongo', 'core_fbj_replenishments_daily_snapshot') }} AS c
    {% if is_incremental() %}
        WHERE
            c.ct >= CURRENT_DATE - INTERVAL 60 DAY
    {% else %}
        WHERE
            c.ct >= DATE("2024-07-01")
    {% endif %}
)
,
status_dt AS (
    SELECT
        _id,
        MAX(IF(t.statusHistory.s = 2, t.statusHistory.ut, NULL)) AS 2_pending_inbound_dt,
        MAX(IF(t.statusHistory.s = 3, t.statusHistory.ut, NULL)) AS 3_pending_shipping_dt,
        MAX(IF(t.statusHistory.s = 4, t.statusHistory.ut, NULL)) AS 4_shipped_dt,
        MAX(IF(t.statusHistory.s = 5, t.statusHistory.ut, NULL)) AS 5_action_required_dt,
        MAX(IF(t.statusHistory.s = 6, t.statusHistory.ut, NULL)) AS 6_on_review_dt,
        MAX_BY(t.statusHistory.s, t.statusHistory.ut) AS last_status,
        MAX_BY(t.statusHistory.src, t.statusHistory.ut) AS last_status_source
    FROM
        status_raw AS t
    GROUP BY
        t._id
)

SELECT
    DATE(r.ct) AS partition_date,
    r._id AS replenishment_id,
    r.vid AS variant_id,
    r.pid AS product_id,
    r.mid AS merchant_id,
    m.merchant_name,
    k.main_merchant_name,
    k.kam,
    k.main_bl AS main_merchant_bl,

    CASE
        WHEN r.src = 1 THEN "Joom"
        WHEN r.src = 2 THEN "Warehouse"
        WHEN r.src = 3 THEN "Merchant"
        ELSE "Source Unknown"
    END AS source,
    CASE
        WHEN s.last_status = 1 THEN "1. Pending Approve"
        WHEN s.last_status = 2 THEN "2. Pending Inbound"
        WHEN s.last_status = 3 THEN "3. Pending Shipping"
        WHEN s.last_status = 4 THEN "4. Shipped by Merchant"
        WHEN s.last_status = 5 THEN "5. Action Required"
        WHEN s.last_status = 6 THEN "6. On Review"
        WHEN s.last_status = 7 THEN "7. Completed"
        WHEN s.last_status = 8 AND s.last_status_source = 1 THEN "8. Canceled by Joom"
        WHEN s.last_status = 8 AND s.last_status_source = 2 THEN "8. Canceled by Merchant"
        ELSE
            "Status Unknown"
    END AS current_status,

    r.ct AS created_at,
    s.2_pending_inbound_dt,
    s.3_pending_shipping_dt,
    s.4_shipped_dt,
    s.5_action_required_dt,
    s.6_on_review_dt,
    IF(
        s.last_status >= 5,
        COALESCE(s.5_action_required_dt, s.6_on_review_dt, last_updated_at),
        NULL
    ) AS completed_dt,

    r.ut AS last_updated_at,
    r.adt AS approve_due_to,
    r.shdt AS shipment_due_to,

    ROUND((UNIX_TIMESTAMP(s.2_pending_inbound_dt) - UNIX_TIMESTAMP(created_at)) / 60 / 60 / 24, 2) AS create_to_approve_days,
    IF(
        s.last_status IN (4, 5, 6, 7) -- считаем метрику только для зашипленных репленишментов
        AND s.4_shipped_dt > s.2_pending_inbound_dt, -- для перестраховки, чтобы не сломать метрику
        ROUND((UNIX_TIMESTAMP(s.4_shipped_dt) - UNIX_TIMESTAMP(s.2_pending_inbound_dt)) / 60 / 60 / 24, 2),
        NULL
    ) AS approve_to_ship_days,
    ROUND((UNIX_TIMESTAMP(completed_dt) - UNIX_TIMESTAMP(created_at)) / 60 / 60 / 24, 2) AS create_to_complete_days,
    r.inbid AS inbound_id,
    r.winbid AS warehouse_inbound_id,
    COALESCE(r.dreq.min, r.min) AS min_count,
    COALESCE(r.dreq.max, r.max) AS max_count,
    r.dreq.reqcnt AS requested_count,
    r.fcnt AS forecasted_count,
    r.whi.scnt AS accepted_count,
    r.whi.pcnt AS problem_count,
    p.business_line,
    p.l1_merchant_category_name,
    p.product_name,
    l.sku AS merchant_sku_id,
    w.extid AS warehouse_sku_id
FROM
    {{ source('mongo', 'core_fbj_replenishments_daily_snapshot') }} AS r
INNER JOIN
    status_dt AS s USING (_id)
LEFT JOIN
    {{ ref('gold_merchants') }} AS m
    ON r.mid = m.merchant_id
LEFT JOIN
    {{ ref('fbj_kam_list') }} AS k
    ON r.mid = k.merchant_id
LEFT JOIN
    {{ ref('gold_products') }} AS p
    ON r.pid = p.product_id
LEFT JOIN
    {{ source('mongo', 'logistics_products_v2_daily_snapshot') }} AS l
    ON r.vid = l.externalid
LEFT JOIN
    {{ source('mongo', 'logistics_warehouse_product_infos_daily_snapshot') }} AS w
    ON l._id = w.pid