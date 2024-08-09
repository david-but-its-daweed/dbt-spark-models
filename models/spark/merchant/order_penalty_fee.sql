{{ config(
    schema='merchant',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov',
      'team': 'merchant',
    }
) }}

SELECT
    entity.payload.joomId AS order_id,
    CAST(SUM(money.sum.amount) AS DOUBLE) AS total_amount,
    CAST(SUM(money.sum.usd) AS DOUBLE) AS total_amount_usd
FROM {{ source('mongo', 'core_merchant_fines_daily_snapshot') }}
WHERE state = 2 AND entity.type = 2 --- state created and type order
GROUP BY entity.payload.joomId
