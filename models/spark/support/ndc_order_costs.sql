{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/'
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts"
     }
 ) }}

WITH t AS (
SELECT
    CURRENT_DATE() AS partition_date,
    t.merchantId AS merchant_id,
    c.name AS merchant_name,
    CASE WHEN t.costs.merchantRevenueInitial.ccy = 'EUR' THEN t.costs.merchantRevenueInsurance.amount * (b.rate / a.rate)
        ELSE t.costs.merchantRevenueInsurance.amount END AS total_insurance,
    CASE WHEN t.costs.merchantRevenueInitial.ccy = 'EUR' THEN t.costs.merchantRevenueInitial.amount * (b.rate / a.rate)
        ELSE t.costs.merchantRevenueInitial.amount END AS revenue_initial
from {{ source('mongo', 'finance_order_costs_daily_snapshot') }} AS t
LEFT JOIN support.cbr_currency_rate AS a ON CURRENT_DATE() = a.partition_date
    AND a.to = 'USD'
LEFT JOIN support.cbr_currency_rate AS b ON CURRENT_DATE() = b.partition_date
    AND b.to = 'EUR'
LEFT JOIN mart.dim_merchant AS c ON t.merchantId = c.merchant_id
WHERE
    t.costs.merchantRevenueInsurance.amount > 0 and
    t.costs.merchantRevenueInsurance.amount > t.costs.merchantRevenueInitial.amount
)

SELECT
    partition_date,
    merchant_id,
    merchant_name,
    ROUND(SUM(total_insurance) - SUM(revenue_initial), 2) as diff
from t
GROUP BY 1, 2, 3
