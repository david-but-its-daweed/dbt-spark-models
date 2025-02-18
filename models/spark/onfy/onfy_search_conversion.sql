{{ config(
    schema='onfy',
    materialized='table',
    file_format='parquet',
    partition_by=['search_event_date'],
    meta = {
      'model_owner' : '@andrewocean',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'search_event_date'
    }
) }}


--------------------------------------------------------------------------------------------------------------------------
-- selecting raw data for user searches, product card openings and product cart addings events
-- events data preprocessing for correct joining
--------------------------------------------------------------------------------------------------------------------------

WITH events_data AS (
    SELECT
        device_id,
        event_id,
        event_ts_cet,
        DATE_TRUNC('DAY', event_ts_cet) AS event_dt,
        payload,
        type
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        TRUE
        AND partition_date_cet >= CURRENT_DATE() - INTERVAL 365 DAYS
        AND type IN ('searchServer', 'productOpen', 'addToCart')
),

searched_products AS (
    SELECT
        device_id,
        event_id,
        event_dt,
        event_ts_cet,
        -- important to separate very close searches in a time frame
        -- adding logic to eliminate cases of too long gap between two searches of the same product
        IF(next_event_ts_cet < event_ts_cet + INTERVAL 2 DAY, next_event_ts_cet, NULL) AS next_event_ts_cet,
        search_query,
        is_category_search,
        -- flatting the table with the product ids from results for joining searches with cart addings
        EXPLODE(product_id) AS product_id,
        search_items_array
    FROM (
        SELECT
            device_id,
            event_id,
            event_dt,
            event_ts_cet,
            -- getting the next search time by the same device_id
            LEAD(event_ts_cet) OVER (PARTITION BY device_id ORDER BY event_ts_cet) AS next_event_ts_cet,
            payload.searchQuery AS search_query,
            payload.categoryId IS NOT NULL AS is_category_search,
            payload.searchItems AS product_id, -- array with products listed in the search results
            -- gathering array with products listed in the search results in case of investigations
            ARRAY_AGG(payload.searchItems) AS search_items_array
        FROM events_data
        WHERE type = 'searchServer'
        GROUP BY 1, 2, 3, 4, 6, 7, 8
    )
),

product_opens AS (
    SELECT
        device_id,
        payload.productId AS product_id,
        payload.productName AS product_name,
        payload.pzn,
        event_dt,
        -- getting time of the first product opening within the day
        MIN(event_ts_cet) AS event_ts_cet,
        -- getting event_id of the first product opening within the day
        MIN_BY(event_id, event_ts_cet) AS event_id
    FROM events_data
    WHERE type = 'productOpen'
    GROUP BY 1, 2, 3, 4, 5
),

cart_addings AS (
    SELECT
        device_id,
        payload.productId AS product_id,
        payload.productName AS product_name,
        payload.pzn,
        event_dt,
        -- getting time of the first product adding to cart within the day
        MIN(event_ts_cet) AS event_ts_cet,
        -- getting event_id of the first product adding to cart within the day
        MIN_BY(event_id, event_ts_cet) AS event_id
    FROM events_data
    WHERE type = 'addToCart'
    GROUP BY 1, 2, 3, 4, 5
),

--------------------------------------------------------------------------------------------------------------------------
-- joining the events according to the flows: SEARCHES -> OPENINGS, and SEARCHES -> CART_ADDINGS -> ORDERS
-- flat dataset with joined events within user searches, groupped for data savings, still has the search results array
--------------------------------------------------------------------------------------------------------------------------

pre_final_flat_table AS (
    SELECT
        -- searches events data
        sp.event_dt AS search_event_dt,
        sp.event_id AS search_event_id,
        sp.event_ts_cet AS search_event_dttm,
        sp.next_event_ts_cet AS search_next_event_dttm,
        sp.search_query,
        sp.is_category_search,
        sp.search_items_array,

        -- touched product (openned, added to cart, bought)
        COALESCE(oi.product_id, ca.product_id, po.product_id) AS product_id,
        -- product_id from orders/addings/openings events
        COALESCE(oi.product_name, ca.product_name, po.product_name) AS product_name,
        -- product_name from orders/addings/openings events
        COALESCE(oi.pzn, ca.pzn, po.pzn) AS pzn,
        -- pzn from orders/addings/openings events

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.product_id AS opening_product_id,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.product_id AS adding_product_id,

        -- orders data
        oi.order_id,
        oi.order_created_time_cet AS order_created_dttm,
        oi.product_id AS order_product_id,
        oi.before_products_price AS order_before_products_price,
        oi.products_price AS order_products_price

    FROM searched_products AS sp

    -- joining SEARCHES with OPENINGS by device_id and product_id
    LEFT JOIN product_opens AS po
        ON
            po.product_id = sp.product_id
            AND po.device_id = sp.device_id
            -- restricting the time window – before next search of the same product or before interval
            -- 99% products addings to cart happens within 30 minutes
            AND po.event_ts_cet
            BETWEEN sp.event_ts_cet AND COALESCE(sp.next_event_ts_cet, sp.event_ts_cet + INTERVAL 30 MINUTE)

    -- joining SEARCHES with CART_ADDINGS by device_id and product_id
    LEFT JOIN cart_addings AS ca
        ON
            ca.product_id = sp.product_id
            AND ca.device_id = sp.device_id
            AND ca.event_ts_cet -- 99% products addings to cart happens within 30 minutes
            BETWEEN sp.event_ts_cet AND COALESCE(sp.next_event_ts_cet, sp.event_ts_cet + INTERVAL 30 MINUTE)

    -- joining CART_ADDINGS with ORDERS by device_id and product_id
    LEFT JOIN {{ source('onfy', 'orders_info') }} AS oi
        ON
            oi.product_id = ca.product_id
            AND oi.device_id = ca.device_id
            AND oi.order_created_time_cet -- 99% products bougth happens within 36 hours
            BETWEEN ca.event_ts_cet AND ca.event_ts_cet + INTERVAL 36 HOUR

    -- grouping to eliminate redundant data due to 20+ products in the search results
    GROUP BY
        sp.event_dt,
        sp.event_id,
        sp.event_ts_cet,
        sp.next_event_ts_cet,
        sp.search_query,
        sp.is_category_search,
        sp.search_items_array,

        COALESCE(oi.product_id, ca.product_id, po.product_id),
        COALESCE(oi.product_name, ca.product_name, po.product_name),
        COALESCE(oi.pzn, ca.pzn, po.pzn),

        po.event_id,
        po.event_ts_cet,
        po.product_id,

        ca.event_id,
        ca.event_ts_cet,
        ca.product_id,

        oi.order_id,
        oi.order_created_time_cet,
        oi.product_id,
        oi.before_products_price,
        oi.products_price

)


--------------------------------------------------------------------------------------------------------------------------
-- in order of data savings preaggregating data for dashboard
--------------------------------------------------------------------------------------------------------------------------

SELECT
    search_event_dt,
    cast(date_trunc('day', search_event_dt) as date) as search_event_date,
    search_query,
    is_category_search,
    product_id,
    product_name,
    pzn,
    COUNT(DISTINCT search_event_id) AS searches,
    COUNT(DISTINCT opening_event_id) AS product_opens,
    COUNT(DISTINCT adding_event_id) AS cart_addings,
    COUNT(DISTINCT order_id) AS orders,
    ROUND(SUM(order_before_products_price), 2) AS order_before_products_price,
    ROUND(SUM(order_products_price), 2) AS order_products_price
FROM pre_final_flat_table
GROUP BY
    search_event_dt,
    cast(date_trunc('day', search_event_dt) as date),
    search_query,
    is_category_search,
    product_id,
    product_name,
    pzn
