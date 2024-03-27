{{
  config(
    meta = {
      'model_owner' : '@gusev',
    },
    materialized='table'
  )
}}

SELECT
    orders.order_date_msk,
    orders.order_id,
    orders.user_id,
    orders.product_id,
    dsa_products.reason
FROM {{ ref('gold_orders') }} AS orders
INNER JOIN {{ ref('dsa_products') }} AS dsa_products USING (product_id)
WHERE orders.order_date_msk >= '2023-01-01'