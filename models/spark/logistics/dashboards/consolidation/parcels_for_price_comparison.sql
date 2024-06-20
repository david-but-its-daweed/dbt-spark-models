{{
  config(
    meta = {
      'model_owner' : '@e.kotsegubov'
    },
    materialized='table',
    schema='jl_models',
  )
}}

SELECT
    fo.tracking_number,
    DATE(DATE_TRUNC('week', MIN(fo.order_created_date_utc))) AS partition_date,
    MAX(fo.country) AS country,
    COLLECT_SET(fo.dangerous_kind) AS dangerous_kinds,
    SUM(fo.gmv_initial) AS gmv_initial,
    MIN(fo.parcel_weight) AS weight,
    SUM(fo.merchant_sale_price) AS merchant_sale_price,
    DATE(DATE_TRUNC('week', MIN(fo.order_created_date_utc))) = DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '3' WEEK AS is_last_week
FROM {{ source ('logistics_mart', 'fact_order') }} AS fo
WHERE
    1 = 1
    AND DATE_TRUNC('week', fo.order_created_date_utc) <= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '3' WEEK
    AND DATE_TRUNC('week', fo.order_created_date_utc) >= DATE_TRUNC('week', DATE '2023-01-01')
    AND fo.origin_country = 'CN'
    AND (
        fo.refund_type IS NULL
        OR fo.refund_type NOT IN ('cancelled_by_user', 'cancelled_by_merchant')
    )
    AND fo.shipping_type != 'Offline'
    AND fo.label_shipping_type != 'Offline'
    AND fo.country != ''
    AND fo.tracking_number IS NOT NULL
    AND fo.logistics_order_id IS NOT NULL
    AND fo.order_weight IS NOT NULL
    AND fo.check_out_time_utc IS NOT NULL
    AND fo.parcel_weight IS NOT NULL
GROUP BY 1
