{{ config(
     schema='support',
     materialized='table',
     location_root='s3://joom-analytics-mart/support/nonpartitioned/',
     file_format='delta',
     meta = {
       'team': 'analytics',
       'bigquery_load': 'true',
       'bigquery_overwrite': 'true',
       'alerts_channel': "#olc_dbt_alerts",
       'model_owner' : '@operational.analytics.duty'
     }
 ) }}

WITH t AS (
    SELECT
        CURRENT_DATE() AS partition_date,
        t.merchantId AS merchant_id,
        c.name AS merchant_name,
        CASE
            WHEN t.costs.merchantRevenueInitial.ccy = 'EUR' THEN t.costs.merchantRevenueInsurance.amount * (to_eur.rate / to_usd.rate)
            ELSE t.costs.merchantRevenueInsurance.amount
        END AS total_insurance,
        CASE
            WHEN t.costs.merchantRevenueInitial.ccy = 'EUR' THEN t.costs.merchantRevenueInitial.amount * (to_eur.rate / to_usd.rate)
            ELSE t.costs.merchantRevenueInitial.amount
        END AS revenue_initial
    FROM {{ source('mongo', 'finance_order_costs_daily_snapshot') }} AS t
    LEFT JOIN support.cbr_currency_rate AS to_usd
        ON CURRENT_DATE() = to_usd.partition_date AND to_usd.`to` = 'USD'
    LEFT JOIN support.cbr_currency_rate AS to_eur
        ON CURRENT_DATE() = to_eur.partition_date AND to_eur.`to` = 'EUR'
    LEFT JOIN mart.dim_merchant AS c
        ON t.merchantId = c.merchant_id
    WHERE
        t.costs.merchantRevenueInsurance.amount > 0
        AND t.costs.merchantRevenueInsurance.amount > t.costs.merchantRevenueInitial.amount
)

SELECT
    partition_date,
    merchant_id,
    merchant_name,
    ROUND(SUM(total_insurance) - SUM(revenue_initial), 2) AS diff
FROM t
GROUP BY 1, 2, 3
