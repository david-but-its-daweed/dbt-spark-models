{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@annzaychik',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring'
    }
) }}

WITH dates AS (
    SELECT DISTINCT
        1 as key,
        DATE(created) AS report_date
    FROM {{ source('pharmacy_landing', 'order') }}
    WHERE DATE(created) >= '2022-06-01'
),

pzns_list as 
(
    SELECT DISTINCT 
        1 as key,
        pzn,
        product_name
    FROM {{ source('onfy', 'orders_info') }}
),

pzns_by_date as 
(
    SELECT 
        report_date,
        pzn,
        product_name
    FROM dates
    JOIN pzns_list
        on dates.key = pzns_list.key
),

stocks_raw AS (
    SELECT
        dates.report_date,
        product.pzn,
        product.manufacturer_name,
        product.store_name,
        SUM(COALESCE(product.stock_quantity, 0)) AS stock_quantity,
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


orders_daily AS (
    SELECT
        pzns_by_date.report_date AS event_date,
        pzns_by_date.pzn,
        pzns_by_date.product_name,
        COALESCE(SUM(orders_info.quantity), 0) AS quantity
    FROM pzns_by_date
    LEFT JOIN {{ source('onfy', 'orders_info') }}
        ON
            pzns_by_date.pzn = orders_info.pzn
            and pzns_by_date.report_date = date(orders_info.order_created_time_cet)
            and orders_info.order_created_time_cet >= '2022-06-01'
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
    orders.quantity AS items_quantity,
    orders.rolling_sum_quantity AS 30d_items_quantity,
    orders.popularity_rank,
    stocks.manufacturer_name,
    stocks.store_name,
    COALESCE(stocks.stock_quantity, 0) AS stock_quantity,
    stocks.price AS item_price
FROM popularity_ranks_orders AS orders
LEFT JOIN stocks_raw AS stocks
    ON
        orders.event_date = stocks.report_date
        AND orders.pzn = stocks.pzn
