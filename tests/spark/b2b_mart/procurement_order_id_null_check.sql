SELECT procurement_order_id
FROM {{ ref('procurement_orders') }}
WHERE procurement_order_id IS NULL
