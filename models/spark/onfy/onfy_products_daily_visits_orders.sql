{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@andrewocean',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_overwrite': 'true'
    }
) }}


WITH products_list AS (
    SELECT
        product_id,
        pzn,
        product_name,
        CONCAT(quantity, ' ', unit) AS package_size,
        manufacturer_short_name as manufacturer
    FROM {{ source('onfy_mart', 'dim_product') }} AS product
    WHERE TRUE
        AND is_current
        AND NOT is_deleted
        AND stock_quantity IS NOT NULL
        AND legal_form != 'RX'
),

visits AS (
    SELECT
        events.payload.productId as product_id,
        DATE_TRUNC('day', event_ts_cet) AS event_dt,
        COUNT(event_id) AS visits
    FROM {{ source('onfy_mart', 'device_events') }} AS events
    WHERE TRUE
        AND partition_date_cet >= CURRENT_DATE() - INTERVAL 730 DAYS
        AND type = 'productOpen'
    GROUP BY 1, 2
), 

orders AS (
    SELECT
        product_id,
        DATE_TRUNC('day', order_created_time_cet) AS order_dt,
        COUNT(DISTINCT order_id) as orders,
        SUM(quantity) as ordered_items,
        SUM(before_products_price) as before_products_price
    FROM {{ ref('orders_info') }}
    WHERE partition_date >= CURRENT_DATE() - INTERVAL 730 DAYS
    GROUP BY 1, 2
),

visits_orders AS (
    SELECT
        COALESCE(visits.product_id, orders.product_id) as product_id,
        COALESCE(visits.event_dt, orders.order_dt) as dt,
        COALESCE(visits, 0) AS visits,
        COALESCE(orders, 0) AS orders,
        COALESCE(ordered_items, 0) AS ordered_items,
        COALESCE(before_products_price, 0.0) AS before_products_price
    FROM visits
    FULL OUTER JOIN orders
        ON visits.product_id = orders.product_id
        AND visits.event_dt = orders.order_dt
)

SELECT 
    products.product_id,
    pzn,
    product_name,
    package_size,
    manufacturer,
    dt,
    visits,
    orders,
    ordered_items,
    before_products_price
FROM products_list AS products
JOIN visits_orders AS visits_orders
    ON products.product_id = visits_orders.product_id
ORDER BY 1, 6
