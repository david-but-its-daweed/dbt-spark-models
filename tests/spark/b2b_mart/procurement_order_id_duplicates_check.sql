SELECT procurement_order_id
FROM {{ source('b2b_mart', 'procurement_orders') }}
GROUP BY procurement_order_id
HAVING COUNT("*") > 1
