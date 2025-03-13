{{
  config(
    materialized='table',
    alias='cancel_rates',
    file_format='parquet',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH calendar AS (
    SELECT
        EXPLODE(
            SEQUENCE(
                TO_DATE('2025-01-01'), DATE_SUB(CURRENT_DATE(), 15), INTERVAL 1 MONTH
            )
        ) AS month_start
),

range_calendar AS (
    SELECT
        month_start AS period_start_date,
        DATE_ADD(month_start, INT(DAY(LAST_DAY(month_start)) / 2) - 1) AS period_end_date
    FROM
        calendar

    UNION ALL

    SELECT
        DATE_ADD(month_start, INT(DAY(LAST_DAY(month_start)) / 2)) AS period_start_date,
        LAST_DAY(month_start) AS period_end_date
    FROM
        calendar
),

metric_notes AS (
    SELECT
        _id AS merchant_order_id,
        metrics.cancelRate.ignored.value AS ignored_value,
        metrics.cancelRate.decision.value AS decision_value
    FROM {{ source('mongo','merchant_order_order_metric_notes_daily_snapshot') }}
),

filtered_orders AS (
    SELECT
        rc.period_start_date,
        rc.period_end_date,

        mo.order_id,
        mn.merchant_order_id,

        mc.business_line,

        mc.l1_merchant_category_name,
        mc.l2_merchant_category_name,
        mc.l3_merchant_category_name,
        mc.l4_merchant_category_name,
        mc.l5_merchant_category_name,

        mo.merchant_id,
        m.origin_name,

        mo.status,

        mn.ignored_value,
        mn.decision_value,

        mo.cancelled_by_merchant_time_utc,
        DATE(mo.created_time_utc) AS created_date
    FROM {{ source('mongo','merchant_order') }} AS mo
    LEFT JOIN metric_notes AS mn
        ON mo.order_id = mn.merchant_order_id
    INNER JOIN gold.merchants AS m
        ON mo.merchant_id = m.merchant_id
    INNER JOIN gold.products AS p
        ON mo.product_id = p.product_id
    INNER JOIN gold.merchant_categories AS mc
        ON p.merchant_category_id = mc.merchant_category_id
    INNER JOIN range_calendar AS rc
        ON DATE(mo.created_time_utc) BETWEEN rc.period_start_date AND rc.period_end_date
    WHERE
        mo.source.kind = 'joom'
        AND rc.period_end_date < DATE_SUB(CURRENT_DATE(), 15)
        AND mn.ignored_value IS NULL OR mn.ignored_value = FALSE
)

SELECT
    merchant_id,

    period_start_date,
    period_end_date,

    origin_name,

    business_line,

    l1_merchant_category_name,
    l2_merchant_category_name,
    l3_merchant_category_name,
    l4_merchant_category_name,
    l5_merchant_category_name,

    COUNT(*) AS orders_count,
    SUM(
        CASE
            WHEN status = 'cancelledByMerchant' AND (decision_value IS NULL OR decision_value = 1) THEN 1
            ELSE 0
        END
    ) AS cancelled_count,
    MAX_BY(order_id, cancelled_by_merchant_time_utc) AS last_cancelled_order_id,
    MAX(cancelled_by_merchant_time_utc) AS last_cancelled_time_utc
FROM filtered_orders
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10


