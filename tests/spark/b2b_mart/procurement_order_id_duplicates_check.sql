SELECT procurement_order_id
FROM {{ ref('procurement_orders') }}
GROUP BY procurement_order_id
HAVING COUNT("*") > 1
