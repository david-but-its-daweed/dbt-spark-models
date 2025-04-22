SELECT procurement_order_id
FROM {{ source('b2b_mart', 'purchasing_and_production_report') }}
WHERE procurement_order_id IS NULL
