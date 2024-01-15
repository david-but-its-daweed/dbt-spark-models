{{ config(
    schema='onfy',
    materialized='table',
    meta = {
      'model_owner' : '@easaltykova',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': 'onfy-etl-monitoring'
    }
) }}

WITH dates AS (
    SELECT DISTINCT DATE(created) AS report_date
    FROM {{ source('pharmacy_landing', 'order') }}
    WHERE DATE(created) >= '2022-06-01'
),

stocks_raw AS (
    SELECT
        dates.report_date,
        product.pzn,
        product.manufacturer_name,
        product.store_name,
        SUM(product.stock_quantity) AS stock_quantity,
        MIN(product.price) AS price
    FROM {{ source('onfy_mart', 'dim_product') }} AS product
    INNER JOIN dates
        ON
            dates.report_date >= DATE(DATE_FORMAT(product.effective_ts, 'yyyy-MM-dd HH:mm:ss'))
            AND dates.report_date < DATE(DATE_FORMAT(product.next_effective_ts, 'yyyy-MM-dd HH:mm:ss'))
    WHERE
        DATE(product.effective_ts) >= '2022-06-01'
        AND product.store_state = 'DEFAULT'
        AND product.product_state = 'DEFAULT'
    GROUP BY 1, 2, 3, 4
),
------------------------------------------------------------------------------------------------------------------------------------

orders_info AS (
    SELECT DISTINCT
        DATE_TRUNC('day', order_created_time_cet) AS order_day,
        pzn,
        product_name
    FROM {{ source('onfy', 'orders_info') }}
    WHERE order_created_time_cet >= '2022-06-01'
),

pzns AS (
    SELECT DISTINCT
        dates.report_date,
        orders_info.pzn,
        orders_info.product_name
    FROM dates
    LEFT JOIN orders_info
        ON dates.report_date BETWEEN (orders_info.order_day - INTERVAL '30 days') AND orders_info.order_day
),

orders_daily AS (
    SELECT
        pzns.report_date AS event_date,
        pzns.pzn,
        pzns.product_name,
        COALESCE(SUM(orders_info.quantity), 0) AS quantity
    FROM pzns
    LEFT JOIN {{ source('onfy', 'orders_info') }}
        ON
            INT(pzns.pzn) = INT(orders_info.pzn)
            AND DATE(pzns.report_date) = DATE(orders_info.order_created_time_cet)
    WHERE
        orders_info.product_id IS NOT NULL
        AND orders_info.order_created_time_cet >= '2022-06-01'
    GROUP BY 1, 2, 3
),

rolling_sum AS (
    SELECT
        orders_daily.*,
        SUM(quantity) OVER (PARTITION BY pzn ORDER BY event_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS rolling_sum_quantity
    FROM orders_daily
),

popularity_ranks_orders AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY event_date ORDER BY rolling_sum_quantity DESC) AS popularity_rank
    FROM rolling_sum
)
------------------------------------------------------------------------------------------------------------------------------------

SELECT
    orders.event_date,
    orders.pzn,
    orders.product_name,
    orders.quantity AS orders_number,
    orders.rolling_sum_quantity AS 30d_orders_number,
    orders.popularity_rank,
    stocks.manufacturer_name,
    stocks.store_name,
    COALESCE(stocks.stock_quantity, 0) AS stock_quantity,
    stocks.price AS min_price
FROM popularity_ranks_orders AS orders
LEFT JOIN stocks_raw AS stocks
    ON
        orders.event_date = stocks.report_date
        AND orders.pzn = stocks.pzn