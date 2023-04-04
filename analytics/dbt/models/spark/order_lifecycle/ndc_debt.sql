{{ config(
    schema='support',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date'],
    meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'bigquery_partitioning_date_column': 'partition_date',
       'alerts_channel': "#olc_dbt_alerts",
       'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

WITH max_shipping_order AS (
    SELECT
        merchant_name,
        MAX(DATE(order_merchant_shipment_time_utc)) AS max_order_merchant_date
    FROM logistics_mart.fact_order
    GROUP BY 1
    )

SELECT
    t.merchant_name,
    CURRENT_DATE() AS partition_date,
    a.max_order_merchant_date,
    COALESCE(ROUND(SUM(t.merchant_insurance)), 0) as merchant_insurance,
    COALESCE(ROUND(SUM(least(t.merchant_insurance, t.merchant_revenue_initial))), 0) as new_merchant_insurance,
    COALESCE(ROUND(SUM(t.merchant_insurance) - SUM(least(t.merchant_insurance, t.merchant_revenue_initial))), 0) as debt
FROM logistics_mart.fact_order AS t
LEFT JOIN max_shipping_order AS a ON t.merchant_name = a.merchant_name
WHERE t.refund_type = 'not_delivered'
GROUP BY 1, 2, 3
HAVING  COALESCE(ROUND(SUM(t.merchant_insurance) - SUM(least(t.merchant_insurance, t.merchant_revenue_initial))), 0) > 0
ORDER BY 1, 2, 3
