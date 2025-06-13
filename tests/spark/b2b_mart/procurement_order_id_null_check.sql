SELECT procurement_order_id
FROM {{ source('b2b_mart', 'procurement_orders') }}
WHERE procurement_order_id IS NULL
