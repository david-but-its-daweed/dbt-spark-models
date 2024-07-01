{{
  config(
    materialized='table',
    meta = {
      'model_owner' : '@general_analytics',
      'priority_weight': '1000',
      'bigquery_load': 'true'
    }
  )
}}

WITH cr_data AS (
    SELECT
        currency_code,
        rate,
        effective_date,
        next_effective_date
    FROM {{ ref('dim_pair_currency_rate') }}
    WHERE
        currency_code_to = 'USD'
)

SELECT
    mo.order_id AS merchant_order_id,
    mo.country AS country_code,
    TO_DATE(mo.marketplace_created_time) AS created_date,
    mo.marketplace_id.marketplace AS marketplace_name,
    mo.product_id,
    mo.money_info.customer_gmv * cr.rate - mo.money_info.customer_vat * cr.rate AS gmv_initial_without_vat
FROM {{ source('mongo', 'merchant_order') }} AS mo
LEFT JOIN cr_data AS cr
    ON
        mo.marketplace_created_time BETWEEN cr.effective_date AND cr.next_effective_date
        AND mo.money_info.merchant_currency = cr.currency_code
WHERE
    source.kind = 'jms'
    AND TO_DATE(mo.marketplace_created_time) >= '2023-02-16' -- date of first JMS order
    AND TO_DATE(mo.marketplace_created_time) < CURRENT_DATE()
