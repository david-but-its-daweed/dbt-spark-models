{{
  config(
    materialized='table',
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true'
    },
  )
}}

WITH aft AS (-- actual fulfillment time
    SELECT
        friendly_id AS friendly_order_id,
        aft / 1000 / 60 / 60 / 24 AS aft -- microsec to days
    FROM {{ source('merchant', 'order_data') }}
),

orders_fulfilled_by_merchants AS (
    SELECT
        o.order_date_msk,
        o.friendly_order_id,
        a.aft AS aft
    FROM {{ ref('gold_orders') }} AS o
    LEFT JOIN aft AS a ON a.friendly_order_id = o.friendly_order_id
    WHERE
        NOT o.is_fbj
        AND o.origin_name = 'Chinese'
        AND o.order_date_msk > '2024-06-15' -- до этой даты нет данных по aft
        AND NOT (o.refund_reason IN ('fraud', 'cancelled_by_customer') AND o.refund_reason IS NOT NULL)
)

SELECT
    order_date_msk,
    PERCENTILE(aft, 0.5) AS merchant_fulfillment_time_p50,
    PERCENTILE(aft, 0.8) AS merchant_fulfillment_time_p80,
    PERCENTILE(aft, 0.95) AS merchant_fulfillment_time_p95
FROM orders_fulfilled_by_merchants
GROUP BY 1