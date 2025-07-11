{{ config(
    schema='onfy',
    materialized='table',
    file_format='delta',
    partition_by=['search_event_date'],
    meta = {
      'model_owner' : '@andrewocean',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'search_event_date',
      'bigquery_overwrite': 'true',
      'bigquery_known_gaps': ['2025-02-28', '2025-03-01', '2025-03-02', '2025-03-03', '2025-03-04', '2025-03-05']
    }
) }}


--------------------------------------------------------------------------------------------------------------------------
-- selecting raw data for orders and user marketing sources
--------------------------------------------------------------------------------------------------------------------------

WITH marketing_sources AS (
    SELECT
        device_id,
        source_corrected AS marketing_source,
        source_dt,
        next_source_dt
    FROM {{ ref('sources') }}
    WHERE next_source_dt >= CURRENT_DATE() - INTERVAL 365 DAYS
),

orders_info AS (
    SELECT
        order_id,
        device_id,
        product_id,
        product_name,
        pzn,
        order_created_time_cet,
        before_products_price,
        products_price
    FROM {{ ref('orders_info') }}
    WHERE order_created_time_cet >= CURRENT_DATE() - INTERVAL 365 DAYS
),

--------------------------------------------------------------------------------------------------------------------------
-- cleaning and preparing the searches data
--------------------------------------------------------------------------------------------------------------------------

search_requests AS (
    SELECT
        device_id,
        device.osType AS platform,
        device_stats.preview.total_num > 0 AS is_not_robot_flg,

        serp_id,
        category_id IS NULL AS is_search_flg,
        partition_date_cet AS event_dt,
        FROM_UTC_TIMESTAMP(event_ts_utc, 'Europe/Berlin') AS event_ts_cet,
        FROM_UTC_TIMESTAMP(
            LEAD(event_ts_utc) OVER (PARTITION BY device_id ORDER BY event_ts_utc),
            'Europe/Berlin'
        ) AS next_event_ts_cet,

        query AS search_query,
        category_id IS NOT NULL AS is_category_search,
        COALESCE(has_product_results, FALSE) AS has_product_results,
        COALESCE(is_suggest, FALSE) AS is_suggest
    FROM {{ source('onfy', 'search_serp_requests_i') }}
    WHERE partition_date_cet >= CURRENT_DATE() - INTERVAL 365 DAYS
),

search_items AS (
    SELECT
        serp_id,
        product_id,
        MAX(sponsored_key IS NOT NULL) AS is_sponsored,
        MIN(COALESCE(v_pos, 0)) AS vertical_position,
        MIN(COALESCE(h_pos, 0)) AS horizontal_position
    FROM {{ source('onfy', 'search_serp_items_i') }}
    WHERE partition_date_cet >= CURRENT_DATE() - INTERVAL 365 DAYS
    GROUP BY serp_id, product_id
),

searched_products AS (
    SELECT
        sr.device_id,
        sr.platform,
        sr.is_not_robot_flg,

        sr.serp_id,
        sr.event_dt,
        sr.event_ts_cet,
        IF(sr.next_event_ts_cet < sr.event_ts_cet + INTERVAL 30 MINUTES, sr.next_event_ts_cet, NULL) AS next_event_ts_cet,

        sr.search_query,
        sr.is_category_search,
        sr.has_product_results,
        sr.is_suggest,
        IF(sr.is_search_flg, 'search', 'catalog') AS search_or_catalog_flg,

        si.product_id,
        si.is_sponsored,
        si.vertical_position,
        si.horizontal_position,
        si.vertical_position + si.horizontal_position AS viewing_position

    FROM search_requests AS sr
    LEFT JOIN search_items AS si
        ON sr.serp_id = si.serp_id
),

--------------------------------------------------------------------------------------------------------------------------
-- selecting raw data for user product openings/addings events, clening and sessionazing data for correct joining
--------------------------------------------------------------------------------------------------------------------------

raw_events AS (
    SELECT
        device_id,
        event_id,
        event_ts_cet,
        DATE_TRUNC('DAY', event_ts_cet) AS event_dt,
        type AS event_type,
        payload.productId AS product_id,
        payload.productName AS product_name,
        payload.pzn
    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= CURRENT_DATE() - INTERVAL 365 DAYS
        AND type IN ('productOpen', 'addToCart')
),

events_with_lag AS (
    SELECT
        *,
        LAG(event_ts_cet) OVER (
            PARTITION BY device_id, product_id, event_type ORDER BY event_ts_cet
        ) AS prev_event_ts_cet
    FROM raw_events
),

events_with_flags AS (
    SELECT
        *,
        -- new session flag: first event or long idle period (30 minutes)
        CASE
            WHEN prev_event_ts_cet IS NULL THEN 1
            WHEN (CAST(event_ts_cet AS LONG) - CAST(prev_event_ts_cet AS LONG)) / 60 > 30 THEN 1
            ELSE 0
        END AS is_new_session
    FROM events_with_lag
),

sessionized_events AS (
    SELECT
        *,
        -- the session number calc within pairs device_id+product_id+event_type
        SUM(is_new_session) OVER (
            PARTITION BY device_id, product_id, event_type ORDER BY event_ts_cet
            ROWS UNBOUNDED PRECEDING
        ) AS session_number
    FROM events_with_flags
),

aggregated_session_events AS (
    SELECT
        device_id,
        product_id,
        pzn,
        event_type,
        session_number,
        -- aggregating time/event_id to the first product opening/adding to cart within session
        MIN(event_ts_cet) AS event_ts_cet,
        MIN_BY(event_id, event_ts_cet) AS event_id,
        -- aggregating product_name in case there were some changes of product data during the session
        MIN_BY(product_name, event_ts_cet) AS product_name
    FROM sessionized_events
    GROUP BY 1, 2, 3, 4, 5
),

product_opens AS (
    SELECT *
    FROM aggregated_session_events
    WHERE event_type = 'productOpen'
),

cart_addings AS (
    SELECT *
    FROM aggregated_session_events
    WHERE event_type = 'addToCart'
),

--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps: SEARCHES -> OPENINGS, SEARCHES -> CART_ADDINGS, CART_ADDINGS -> ORDERS
--------------------------------------------------------------------------------------------------------------------------

-- 1. Search → Product Openings (within 30 min)
search_to_openings AS (
    SELECT
        sp.device_id,
        sp.event_dt AS search_event_dt,
        sp.serp_id AS search_event_id,
        sp.event_ts_cet AS search_event_dttm,
        sp.next_event_ts_cet AS search_next_event_dttm,
        sp.product_id,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.pzn,
        po.product_name
    FROM searched_products AS sp
    INNER JOIN product_opens AS po
        ON
            sp.device_id = po.device_id
            AND sp.product_id = po.product_id
            AND po.event_ts_cet -- 99% products opening happens within 30 minutes
            BETWEEN sp.event_ts_cet AND COALESCE(sp.next_event_ts_cet, sp.event_ts_cet + INTERVAL 30 MINUTE)
),

-- 2. Search → Cart Addings (within 30 min)
search_to_cart_addings AS (
    SELECT
        sp.device_id,
        sp.event_dt AS search_event_dt,
        sp.serp_id AS search_event_id,
        sp.event_ts_cet AS search_event_dttm,
        sp.next_event_ts_cet AS search_next_event_dttm,
        sp.product_id,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_name
    FROM searched_products AS sp
    INNER JOIN cart_addings AS ca
        ON
            sp.device_id = ca.device_id
            AND sp.product_id = ca.product_id
            AND ca.event_ts_cet -- 99% products addings to cart happens within 30 minutes
            BETWEEN sp.event_ts_cet AND COALESCE(sp.next_event_ts_cet, sp.event_ts_cet + INTERVAL 30 MINUTE)
),

-- 3. Cart Addings → Orders (within 36 hours)
cart_addings_to_orders AS (
    SELECT
        ca.event_id AS adding_event_id,
        ca.device_id,
        ca.product_id,
        ca.event_ts_cet AS adding_event_dttm,

        oi.order_id,
        oi.pzn,
        oi.product_name,
        oi.order_created_time_cet AS order_created_dttm,
        oi.before_products_price AS order_before_products_price,
        oi.products_price AS order_products_price
    FROM cart_addings AS ca
    INNER JOIN orders_info AS oi
        ON
            ca.device_id = oi.device_id
            AND ca.product_id = oi.product_id
            AND oi.order_created_time_cet -- 99% products bougth happens within 36 hours
            BETWEEN ca.event_ts_cet AND ca.event_ts_cet + INTERVAL 36 HOUR
),

--------------------------------------------------------------------------------------------------------------------------
-- joining the events according to the flows: SEARCHES -> OPENINGS and SEARCHES -> CART_ADDINGS -> ORDERS
-- flat dataset with joined events within user searches, groupped for data savings
--------------------------------------------------------------------------------------------------------------------------

pre_final_flat_table AS (
    SELECT
        -- device data
        sp.platform,
        sp.is_not_robot_flg,
        MIN_BY(src.marketing_source, src.source_dt) AS marketing_source,

        -- searches and products data
        sp.event_dt AS search_event_dt,
        sp.serp_id AS search_event_id,
        sp.event_ts_cet AS search_event_dttm,
        sp.next_event_ts_cet AS search_next_event_dttm,
        sp.search_query,
        sp.is_category_search,
        sp.has_product_results,
        sp.is_suggest,
        sp.search_or_catalog_flg,
        sp.is_sponsored,

        -- touched product (openned, added to cart, bought)
        COALESCE(ato.product_id, sta.product_id, sto.product_id) AS product_id,
        COALESCE(ato.product_name, sta.product_name, sto.product_name) AS product_name,
        COALESCE(ato.pzn, sta.pzn, sto.pzn) AS pzn,

        -- product openings events data
        sto.opening_event_id,
        sto.opening_event_dttm,

        -- cart addings events data
        sta.adding_event_id,
        sta.adding_event_dttm,

        -- orders data
        ato.order_id,
        ato.order_created_dttm,
        ato.order_before_products_price,
        ato.order_products_price

    FROM searched_products AS sp

    -- join marketing sources
    LEFT JOIN marketing_sources AS src
        ON
            sp.device_id = src.device_id
            AND sp.event_ts_cet
            BETWEEN src.source_dt AND src.next_source_dt

    -- join search → opening CTE
    LEFT JOIN search_to_openings AS sto
        ON
            sp.serp_id = sto.search_event_id
            AND sp.device_id = sto.device_id
            AND sp.product_id = sto.product_id

    -- join search → cart addings CTE
    LEFT JOIN search_to_cart_addings AS sta
        ON
            sp.serp_id = sta.search_event_id
            AND sp.device_id = sta.device_id
            AND sp.product_id = sta.product_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS ato
        ON
            sta.adding_event_id = ato.adding_event_id
            AND sta.device_id = ato.device_id
            AND sta.product_id = ato.product_id

    WHERE
        TRUE
        AND sp.event_dt IS NOT NULL
        AND DATE(sp.event_dt) < CURRENT_DATE()

    -- grouping to eliminate redundancy due to product listing
    GROUP BY
        sp.platform,
        sp.is_not_robot_flg,

        sp.event_dt,
        sp.serp_id,
        sp.event_ts_cet,
        sp.next_event_ts_cet,
        sp.search_query,
        sp.is_category_search,
        sp.has_product_results,
        sp.is_suggest,
        sp.search_or_catalog_flg,
        sp.is_sponsored,

        COALESCE(ato.product_id, sta.product_id, sto.product_id),
        COALESCE(ato.product_name, sta.product_name, sto.product_name),
        COALESCE(ato.pzn, sta.pzn, sto.pzn),

        sto.opening_event_id,
        sto.opening_event_dttm,

        sta.adding_event_id,
        sta.adding_event_dttm,

        ato.order_id,
        ato.order_created_dttm,
        ato.order_before_products_price,
        ato.order_products_price
)

--------------------------------------------------------------------------------------------------------------------------
-- the proper aggregation is not possible – metrics become non-additive within different dimension aggregation
--------------------------------------------------------------------------------------------------------------------------

SELECT
    search_event_dt,
    CAST(DATE_TRUNC('day', search_event_dt) AS DATE) AS search_event_date,
    search_event_id,
    search_query,
    is_category_search,
    has_product_results,
    is_suggest,
    search_or_catalog_flg,

    platform,
    is_not_robot_flg,
    marketing_source,

    product_id,
    product_name,
    pzn,
    is_sponsored,

    opening_event_id,
    adding_event_id,
    order_id,

    ROUND(SUM(order_before_products_price), 2) AS order_before_products_price,
    ROUND(SUM(order_products_price), 2) AS order_products_price

FROM pre_final_flat_table

GROUP BY
    search_event_dt,
    CAST(DATE_TRUNC('day', search_event_dt) AS DATE),
    search_event_id,
    search_query,
    is_category_search,
    has_product_results,
    is_suggest,
    search_or_catalog_flg,

    platform,
    is_not_robot_flg,
    marketing_source,

    product_id,
    product_name,
    pzn,
    is_sponsored,

    opening_event_id,
    adding_event_id,
    order_id

DISTRIBUTE BY search_event_date
