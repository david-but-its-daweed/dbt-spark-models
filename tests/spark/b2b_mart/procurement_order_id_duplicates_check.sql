SELECT procurement_order_id
FROM {{ source('b2b_mart', 'purchasing_and_production_report') }}
GROUP BY procurement_order_id
HAVING COUNT("*") > 1
