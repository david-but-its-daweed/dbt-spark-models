{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@catman-analytics.duty',
      'priority_weight': '1000',
      'bigquery_load': 'true'
    }
  )
}}


WITH logistics_status_history AS (
    SELECT
        EXPLODE(statusHistory) AS sh_raw, 
        *
    FROM {{ source('mongo', 'logistics_replenishments_v3_daily_snapshot') }} -- таблица с информацией о приезде мерчантских поставок на сток
),

logistics_boxes AS (
    SELECT
        sh_raw.uTm AS completed_at,
        EXPLODE(boxes) boxes_raw,
        *
    FROM logistics_status_history AS 
    WHERE sh_raw.status = 30 -- поставка приехала на склад
), 

logistics_stock AS (
    SELECT 
        EXPLODE(boxes_raw.stocks) AS stock,
        *
    FROM logistics_boxes 
), 

first_time_in_stock AS (
    SELECT
        stock.extId AS product_variant_id,
        min(completed_at) AS first_time_in_stock -- самая первая дата поставки по этому продуктовому варианту 
    FROM logistics_stock
    GROUP BY 1
)

SELECT
    DATE(fo.order_fulfilled_online_time_utc) AS order_fulfilled_date_utc,
    fo.order_number,
    fo.order_id,
    fo.merchant_id,
    fo.is_fbj_order
FROM {{ source('logistics_mart', 'fact_order') }} AS fo
INNER JOIN first_time_in_stock AS ft ON ft.product_variant_id = fo.product_variant_id AND fo.order_fulfilled_online_time_utc > ft.first_time_in_stock
INNER JOIN {{ source('mongo', 'product_fbj_demand_sk_us_daily_snapshot') }} AS a ON a._id = ft.product_variant_id
WHERE
    fo.warehouse_country = 'CN'
    AND fo.order_number IS NOT NULL
    AND fo.order_cancellation_time_utc IS NULL
    AND fo.order_created_date_utc>'2024-07-10'
    AND (a.enabled OR (NOT a.enabled AND order_fulfilled_online_time_utc < a.dt))