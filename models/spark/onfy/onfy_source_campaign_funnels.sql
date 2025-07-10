{{ config(
    schema='onfy',
    materialized='table',
    file_format='delta',
    partition_by=['event_date'],
    meta = {
      'model_owner' : '@andrewocean',
      'team': 'onfy',
      'bigquery_load': 'true',
      'alerts_channel': '#onfy-etl-monitoring',
      'bigquery_partitioning_date_column': 'event_date',
      'bigquery_overwrite': 'true'
    }
) }}



--------------------------------------------------------------------------------------------------------------------------
-- selecting raw data for orders
--------------------------------------------------------------------------------------------------------------------------
WITH orders_info AS (
    SELECT
        order_id,
        device_id,
        order_created_time_cet,
        product_id,
        product_name,
        pzn,
        SUM(quantity) AS quantity,
        SUM(before_products_price) AS before_products_price,
        SUM(products_price) AS products_price
    FROM {{ ref('orders_info') }}
    WHERE order_created_time_cet >= CURRENT_DATE() - INTERVAL 120 DAYS
    GROUP BY
        order_id,
        device_id,
        order_created_time_cet,
        product_id,
        product_name,
        pzn
),


--------------------------------------------------------------------------------------------------------------------------
-- selecting raw events data, clening and sessionazing for correct joining
--------------------------------------------------------------------------------------------------------------------------
raw_events AS (
    SELECT
        device_id,
        event_id,
        event_ts_cet,
        partition_date_cet AS event_dt,
        type AS event_type,

        payload.productId AS product_id,
        payload.productName AS product_name,
        payload.pzn,

        payload.sourceScreen AS source_screen,
        payload.widgetType AS widget_type,
        payload.recommendationType AS recommendation_type,
        payload.recommendationSlotName AS recommendation_slot,
        payload.promoKey AS promo_key,
        payload.blockName AS block_name,
        payload.isSponsored AS is_sponsored,
        payload.params.utm_campaign,
        payload.params.utm_medium,
        payload.params.utm_source,
        payload.alternativeProductId AS alternative_product_id

    FROM {{ source('onfy_mart', 'device_events') }}
    WHERE
        partition_date_cet >= CURRENT_DATE() - INTERVAL 120 DAYS
        AND type IN (
            'productPreview', 'homeOpen', 'productOpen', 'cartOpen', 'addToCart',
            'producerBannerShown', 'producerBannerClicked', 'externalLink', 'productAlternativePopupShown'
        )
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
        CASE
            WHEN event_type = 'productPreview' THEN 'preview'
            WHEN event_type = 'homeOpen' THEN 'home'
            WHEN event_type = 'productOpen' THEN 'product'
            WHEN event_type = 'cartOpen' THEN 'cart'
            WHEN event_type = 'addToCart' THEN 'adding'
            WHEN event_type = 'producerBannerShown' THEN 'banner_show'
            WHEN event_type = 'producerBannerClicked' THEN 'banner_click'
            WHEN event_type = 'externalLink' THEN 'crm_email'
            WHEN event_type = 'productAlternativePopupShown' THEN 'popup'
        END AS event_type,
        session_number,

        -- aggregating event_id/time to the first within session
        MIN_BY(event_id, event_ts_cet) AS event_id,
        MIN(event_dt) AS event_dt,
        MIN(event_ts_cet) AS event_ts_cet,

        product_id,
        pzn,
        -- aggregating product_name in case there were some changes of product data during the session
        MIN_BY(product_name, event_ts_cet) AS product_name,

        source_screen,
        widget_type,
        recommendation_type,
        recommendation_slot,
        promo_key,
        block_name,
        is_sponsored,
        utm_campaign,
        utm_medium,
        utm_source,
        alternative_product_id

    FROM sessionized_events
    GROUP BY
        device_id,
        CASE
            WHEN event_type = 'productPreview' THEN 'preview'
            WHEN event_type = 'homeOpen' THEN 'home'
            WHEN event_type = 'productOpen' THEN 'product'
            WHEN event_type = 'cartOpen' THEN 'cart'
            WHEN event_type = 'addToCart' THEN 'adding'
            WHEN event_type = 'producerBannerShown' THEN 'banner_show'
            WHEN event_type = 'producerBannerClicked' THEN 'banner_click'
            WHEN event_type = 'externalLink' THEN 'crm_email'
            WHEN event_type = 'productAlternativePopupShown' THEN 'popup'
        END,
        session_number,

        product_id,
        pzn,

        source_screen,
        widget_type,
        recommendation_type,
        recommendation_slot,
        promo_key,
        block_name,
        is_sponsored,
        utm_campaign,
        utm_medium,
        utm_source,
        alternative_product_id
),


--------------------------------------------------------------------------------------------------------------------------
-- cleaning and preparing the searches and catalogs data
--------------------------------------------------------------------------------------------------------------------------
search_catalog_requests AS (
    SELECT
        device_id,
        serp_id,
        (category_id IS NULL AND query IS NOT NULL) AS is_search_flg,
        partition_date_cet AS event_dt,
        FROM_UTC_TIMESTAMP(event_ts_utc, 'Europe/Berlin') AS event_ts_cet,
        FROM_UTC_TIMESTAMP(
            LEAD(event_ts_utc) OVER (PARTITION BY device_id ORDER BY event_ts_utc),
            'Europe/Berlin'
        ) AS next_event_ts_cet,
        query AS search_query,
        category_id
    FROM {{ source('onfy', 'search_serp_requests_i') }}
    WHERE partition_date_cet >= CURRENT_DATE() - INTERVAL 120 DAYS
),

requests_items AS (
    SELECT
        serp_id,
        product_id,
        MAX(has_preview) AS has_preview,
        MAX(sponsored_key IS NOT NULL) AS is_sponsored,
        MODE(sponsored_key) AS sponsored_key
    FROM {{ source('onfy', 'search_serp_items_i') }}
    WHERE partition_date_cet >= CURRENT_DATE() - INTERVAL 120 DAYS AND product_id IS NOT NULL
    GROUP BY serp_id, product_id
),

search_catalog_products AS (
    SELECT
        sr.device_id,
        sr.serp_id AS initial_event_id,
        IF(sr.is_search_flg, 'search', 'catalog') AS event_type,
        sr.event_dt,
        sr.event_ts_cet,
        IF(sr.next_event_ts_cet < sr.event_ts_cet + INTERVAL 30 MINUTES, sr.next_event_ts_cet, NULL) AS next_event_ts_cet,
        sr.search_query,
        sr.category_id,
        ri.product_id,
        ri.has_preview,
        ri.is_sponsored,
        ri.sponsored_key
    FROM search_catalog_requests AS sr
    LEFT JOIN requests_items AS ri
        ON sr.serp_id = ri.serp_id
    WHERE ri.has_preview
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps - SOURCES:
-- SEARCH -> PREVIEW,       SEARCH -> OPENING,      SEARCH -> CART_ADDING
-- CATALOG -> PREVIEW,      CATALOG -> OPENING,     CATALOG -> CART_ADDING
--------------------------------------------------------------------------------------------------------------------------
product_previews AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'preview'
),

product_opens AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'product'
),

cart_addings AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'adding'
),

-- selecting dictionary for catalog categories
categories AS (
    SELECT
        id,
        name
    FROM {{ source('pharmacy_landing', 'category') }}
),


-- Product Previews ← Initial Search/Catalog Event (within 30 min)
previews_to_source AS (
    SELECT
        pp.event_dt,
        pp.event_id AS preview_event_id,
        pp.event_ts_cet AS preview_event_dttm,
        pp.pzn,
        pp.product_id,
        pp.product_name,

        MIN_BY(sp.event_type, pp.event_ts_cet) AS event_type,
        MIN_BY(sp.sponsored_key, pp.event_ts_cet) AS sponsored_key,
        MIN_BY(sp.search_query, pp.event_ts_cet) AS search_query,
        MIN_BY(sp.category_id, pp.event_ts_cet) AS category_id,
        MIN_BY(ctg.name, pp.event_ts_cet) AS category_name
    FROM product_previews AS pp
    LEFT JOIN search_catalog_products AS sp
        ON
            pp.device_id = sp.device_id
            AND pp.product_id = sp.product_id
            AND pp.event_ts_cet >= sp.event_ts_cet
            AND pp.event_ts_cet < COALESCE(sp.next_event_ts_cet, sp.event_ts_cet + INTERVAL 30 MINUTE)
    -- category names dict
    LEFT JOIN categories AS ctg
        ON sp.category_id = ctg.id
    WHERE pp.widget_type IN ('searchProduct', 'searchCarousel', '')
    GROUP BY
        pp.event_dt,
        pp.event_id,
        pp.event_ts_cet,
        pp.pzn,
        pp.product_id,
        pp.product_name
),

-- Product Previews → Product Openings from Search (within 30 min)
preview_to_openings AS (
    SELECT
        pp.event_type,
        pp.event_id AS preview_event_id,
        pp.event_ts_cet AS preview_event_dttm,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.pzn,
        po.product_id,
        po.product_name
    FROM product_previews AS pp
    INNER JOIN product_opens AS po
        ON
            pp.device_id = po.device_id
            AND pp.product_id = po.product_id
            AND pp.event_ts_cet <= po.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 30 MINUTE) > po.event_ts_cet
    WHERE po.widget_type IN ('searchProduct', 'searchCarousel', '')
),

-- Product Previews → Cart Addings not from Recommendations (within 30 min)
preview_to_cart_addings AS (
    SELECT
        pp.event_type,
        pp.event_id AS preview_event_id,
        pp.event_ts_cet AS preview_event_dttm,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_id,
        ca.product_name
    FROM product_previews AS pp
    INNER JOIN cart_addings AS ca
        ON
            pp.device_id = ca.device_id
            AND pp.product_id = ca.product_id
            AND pp.event_ts_cet <= ca.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 30 MINUTE) > ca.event_ts_cet
    WHERE ca.widget_type NOT IN ('recommendations', 'recommendation', 'previouslyBought', 'alternativesPopup')
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps - SOURCES:
-- PREVIEW -> OPENING,  PREVIEW -> CART_ADDING,  PREVIEW -> OPENING -> CART_ADDING
--------------------------------------------------------------------------------------------------------------------------
product_previews_from_recomendations AS (
    SELECT
        *,
        event_id AS preview_event_id,
        event_ts_cet AS preview_event_dttm,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'preview' AND widget_type IN ('recommendations', 'recommendation')
),

-- Product Previews from Recommendations → Product Opens (within 30 min)
recom_product_previews_to_openings AS (
    SELECT
        pp.event_id AS preview_event_id,
        pp.event_dt,
        pp.device_id,
        pp.source_screen,
        pp.recommendation_type,
        pp.promo_key,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.pzn,
        po.product_id,
        po.product_name
    FROM product_previews_from_recomendations AS pp
    INNER JOIN product_opens AS po
        ON
            pp.device_id = po.device_id
            AND pp.product_id = po.product_id
            AND pp.source_screen = po.source_screen
            AND pp.widget_type = po.widget_type
            AND pp.event_ts_cet <= po.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 30 MINUTE) > po.event_ts_cet
),

-- Product Previews from Recommendations → Cart Addings (within 30 min)
recom_product_previews_to_cart_addings AS (
    SELECT
        pp.event_id AS preview_event_id,
        pp.device_id,
        pp.source_screen,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_id,
        ca.product_name
    FROM product_previews_from_recomendations AS pp
    INNER JOIN cart_addings AS ca
        ON
            pp.device_id = ca.device_id
            AND pp.product_id = ca.product_id
            AND pp.source_screen = ca.source_screen
            AND pp.widget_type = ca.widget_type
            AND pp.event_ts_cet <= ca.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 30 MINUTE) > ca.event_ts_cet
),

recom_product_preview_to_product_open_to_cart_addings AS (
    SELECT
        pp.event_id AS preview_event_id,
        pp.device_id,
        pp.source_screen,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_id,
        ca.product_name
    FROM product_previews_from_recomendations AS pp
    INNER JOIN product_opens AS po
        ON
            pp.device_id = po.device_id
            AND pp.product_id = po.product_id
            AND pp.source_screen = po.source_screen
            AND pp.widget_type = po.widget_type
            AND pp.event_ts_cet <= po.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 30 MINUTE) > po.event_ts_cet
    INNER JOIN cart_addings AS ca
        ON
            po.device_id = ca.device_id
            AND po.product_id = ca.product_id
            AND po.source_screen = ca.source_screen
            AND po.widget_type = ca.widget_type
            AND po.event_ts_cet <= ca.event_ts_cet
            AND COALESCE(po.next_event_ts_cet, po.event_ts_cet + INTERVAL 30 MINUTE) > ca.event_ts_cet
),

-- Recommendations → Cart Addings (UNION)
recommendations_to_cart_addings AS (
    SELECT
        preview_event_id,
        device_id,
        source_screen,

        -- cart addings events data
        adding_event_id,
        adding_event_dttm,
        pzn,
        product_id,
        product_name
    FROM recom_product_previews_to_cart_addings

    UNION ALL

    SELECT
        preview_event_id,
        device_id,
        source_screen,

        -- cart addings events data
        adding_event_id,
        adding_event_dttm,
        pzn,
        product_id,
        product_name
    FROM recom_product_preview_to_product_open_to_cart_addings

    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps - SOURCES:
-- BANNER <- INITIAL,   BANNER -> OPENING,   OPENING -> CART_ADDING
--------------------------------------------------------------------------------------------------------------------------
banner_shows AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'banner_show'
),

banner_clicks AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'banner_click'
),

banner_initial_events AS (
    SELECT
        event_type,
        device_id,
        initial_event_id,
        event_ts_cet,
        next_event_ts_cet
    FROM search_catalog_products
    UNION ALL
    SELECT
        event_type,
        device_id,
        event_id AS initial_event_id,
        event_ts_cet,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'home'
    UNION ALL
    SELECT
        event_type,
        device_id,
        event_id AS initial_event_id,
        event_ts_cet,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'product'
    UNION ALL
    SELECT
        event_type,
        device_id,
        event_id AS initial_event_id,
        event_ts_cet,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'cart'
),

-- Banner Shows ← Banner Initial Event (within 5 min)
banner_shows_to_source_screen AS (
    SELECT
        bs.event_dt,
        bs.event_id AS preview_event_id,
        bs.event_ts_cet AS preview_event_dttm,
        bs.block_name,

        -- banner source by pre event
        MIN_BY(be.event_type, be.event_ts_cet) AS source_screen
    FROM banner_shows AS bs
    LEFT JOIN banner_initial_events AS be
        ON
            bs.device_id = be.device_id
            AND bs.event_ts_cet >= be.event_ts_cet
            AND bs.event_ts_cet < COALESCE(be.next_event_ts_cet, be.event_ts_cet + INTERVAL 5 MINUTE)
    GROUP BY 1, 2, 3, 4
),

-- Banner Shows → Banner Clicks → Product Opens (within 30 min)
banner_shows_to_openings AS (
    SELECT
        bs.event_type,
        bs.device_id,
        bs.event_id AS preview_event_id,
        bs.event_ts_cet AS preview_event_dttm,
        bs.event_dt,
        bs.block_name,

        -- banner clicks events data
        bc.event_id AS click_event_id,
        bc.event_ts_cet AS click_event_dttm,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.pzn,
        po.product_id,
        po.product_name
    FROM banner_shows AS bs
    INNER JOIN banner_clicks AS bc
        ON
            bs.device_id = bc.device_id
            AND bs.block_name = bc.block_name
            AND bs.event_ts_cet <= bc.event_ts_cet
            AND COALESCE(bs.next_event_ts_cet, bs.event_ts_cet + INTERVAL 30 MINUTE) > bc.event_ts_cet
    INNER JOIN product_opens AS po
        ON
            bc.device_id = po.device_id
            AND bc.source_screen = po.source_screen
            AND bc.event_ts_cet <= po.event_ts_cet
            AND COALESCE(bc.next_event_ts_cet, bc.event_ts_cet + INTERVAL 2 MINUTE) > po.event_ts_cet
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps - SOURCES:
-- EMAIL -> OPENING
--------------------------------------------------------------------------------------------------------------------------
crm_emails AS (
    SELECT
        *,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'crm_email'
),

-- Email Clicks → Product Opens (within 2 min)
email_click_to_openings AS (
    SELECT
        ce.event_type,
        ce.device_id,
        ce.event_dt,
        ce.event_ts_cet AS preview_event_dttm,
        ce.event_id AS preview_event_id,
        ce.utm_campaign,

        -- product openings events data
        po.event_id AS opening_event_id,
        po.event_ts_cet AS opening_event_dttm,
        po.pzn,
        po.product_id,
        po.product_name
    FROM crm_emails AS ce
    INNER JOIN product_opens AS po
        ON
            ce.device_id = po.device_id
            AND ce.event_ts_cet <= po.event_ts_cet
            AND COALESCE(ce.next_event_ts_cet, ce.event_ts_cet + INTERVAL 2 MINUTE) > po.event_ts_cet
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps - SOURCES:
-- POPUP -> CART_ADDING
--------------------------------------------------------------------------------------------------------------------------
popups AS (
    SELECT
        *,
        event_id AS opening_event_id,
        event_ts_cet AS opening_event_dttm,
        LEAD(event_ts_cet) OVER (PARTITION BY device_id, product_id ORDER BY event_ts_cet) AS next_event_ts_cet
    FROM aggregated_session_events
    WHERE event_type = 'popup'
),

-- Popup Shows → Product Addings (within 2 min)
popup_shows_to_addings AS (
    SELECT
        pp.event_type,
        pp.device_id,
        pp.event_dt,
        pp.event_id AS opening_event_id,
        pp.event_ts_cet AS opening_event_dttm,

        -- product addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_id,
        ca.product_name
    FROM popups AS pp
    LEFT JOIN cart_addings AS ca
        ON
            pp.device_id = ca.device_id
            AND pp.alternative_product_id = ca.product_id
            AND pp.event_ts_cet <= ca.event_ts_cet
            AND COALESCE(pp.next_event_ts_cet, pp.event_ts_cet + INTERVAL 2 MINUTE) > ca.event_ts_cet
    WHERE ca.widget_type = 'alternativesPopup'
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing modular CTEs with funnel steps for initial steps:
-- PRODUCT_OPENS -> CART_ADDINGS        CART_ADDINGS -> ORDERS
--------------------------------------------------------------------------------------------------------------------------

-- Product Opens → Addings to Cart (within 30 min)
openings_to_cart_addings AS (
    SELECT
        po.event_type,
        po.device_id,
        po.event_id AS opening_event_id,

        -- cart addings events data
        ca.event_id AS adding_event_id,
        ca.event_ts_cet AS adding_event_dttm,
        ca.pzn,
        ca.product_id,
        ca.product_name
    FROM product_opens AS po
    INNER JOIN cart_addings AS ca
        ON
            po.device_id = ca.device_id
            AND po.event_ts_cet <= ca.event_ts_cet
            AND COALESCE(po.next_event_ts_cet, po.event_ts_cet + INTERVAL 30 MINUTE) > ca.event_ts_cet
    WHERE ca.widget_type NOT IN ('recommendations', 'recommendation') -- without recommendations addings
),

-- Cart Addings → Orders (within 36 hours)
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
        oi.products_price AS order_products_price,
        oi.quantity AS order_quantity
    FROM cart_addings AS ca
    INNER JOIN orders_info AS oi
        ON
            ca.device_id = oi.device_id
            AND ca.product_id = oi.product_id
            AND oi.order_created_time_cet -- 99% products bougth happens within 36 hours
            BETWEEN ca.event_ts_cet AND ca.event_ts_cet + INTERVAL 36 HOUR
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing funnel for all possible scenarios:
-- SEARCH, CATALOG, RECOMMENDATION, BANNER, EMAIL, POPUP
--------------------------------------------------------------------------------------------------------------------------

pre_final_flat_table AS (
-- flat table for search and catalog events
    SELECT
        ps.event_dt,
        ps.event_type AS source,
        ps.event_type AS first_page,
        COALESCE(ps.search_query, ps.category_name) AS placement,
        ps.sponsored_key AS campaign_name,
        COALESCE(a2o.order_created_dttm, p2a.adding_event_dttm, p2o.opening_event_dttm, ps.preview_event_dttm) AS source_event_ts_cet,

        COALESCE(a2o.product_id, p2a.product_id, p2o.product_id, ps.product_id) AS product_id,
        COALESCE(a2o.product_name, p2a.product_name, p2o.product_name) AS product_name,
        COALESCE(a2o.pzn, p2a.pzn, p2o.pzn, ps.pzn) AS pzn,

        ps.preview_event_id,
        p2o.opening_event_id,
        p2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    FROM previews_to_source AS ps

    -- join initial → opening CTE
    LEFT JOIN preview_to_openings AS p2o
        ON ps.preview_event_id = p2o.preview_event_id

    -- join initial → cart addings CTE
    LEFT JOIN preview_to_cart_addings AS p2a
        ON ps.preview_event_id = p2a.preview_event_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS a2o
        ON p2a.adding_event_id = a2o.adding_event_id

    GROUP BY
        ps.event_dt,
        ps.event_type,
        ps.event_type,
        COALESCE(ps.search_query, ps.category_name),
        ps.sponsored_key,
        COALESCE(a2o.order_created_dttm, p2a.adding_event_dttm, p2o.opening_event_dttm, ps.preview_event_dttm),

        COALESCE(a2o.product_id, p2a.product_id, p2o.product_id, ps.product_id),
        COALESCE(a2o.product_name, p2a.product_name, p2o.product_name),
        COALESCE(a2o.pzn, p2a.pzn, p2o.pzn, ps.pzn),

        ps.preview_event_id,
        p2o.opening_event_id,
        p2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    UNION ALL

    -- flat table for recommendation events
    SELECT
        pr.event_dt,
        'recommendation' AS source,
        pr.source_screen AS first_page,
        pr.recommendation_type AS placement,
        pr.promo_key AS campaign_name,
        COALESCE(a2o.order_created_dttm, r2a.adding_event_dttm, p2o.opening_event_dttm, pr.preview_event_dttm) AS source_event_ts_cet,

        COALESCE(a2o.product_id, r2a.product_id, p2o.product_id, pr.product_id) AS product_id,
        COALESCE(a2o.product_name, r2a.product_name, p2o.product_name) AS product_name,
        COALESCE(a2o.pzn, r2a.pzn, p2o.pzn, pr.pzn) AS pzn,

        pr.preview_event_id,
        p2o.opening_event_id,
        r2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity


    FROM product_previews_from_recomendations AS pr

    -- join preview → product openings CTE
    LEFT JOIN recom_product_previews_to_openings AS p2o
        ON pr.preview_event_id = p2o.preview_event_id

    -- join preview → cart addings CTE
    LEFT JOIN recommendations_to_cart_addings AS r2a
        ON p2o.preview_event_id = r2a.preview_event_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS a2o
        ON r2a.adding_event_id = a2o.adding_event_id

    GROUP BY

        pr.event_dt,
        pr.source_screen,
        pr.recommendation_type,
        pr.promo_key,
        COALESCE(a2o.order_created_dttm, r2a.adding_event_dttm, p2o.opening_event_dttm, pr.preview_event_dttm),

        COALESCE(a2o.product_id, r2a.product_id, p2o.product_id, pr.product_id),
        COALESCE(a2o.product_name, r2a.product_name, p2o.product_name),
        COALESCE(a2o.pzn, r2a.pzn, p2o.pzn, pr.pzn),

        pr.preview_event_id,
        p2o.opening_event_id,
        r2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    UNION ALL

    -- flat table for banner events
    SELECT
        bs.event_dt,
        'banner' AS source,
        bs.source_screen AS first_page,
        NULL AS placement,
        bs.block_name AS campaign_name,
        COALESCE(a2o.order_created_dttm, o2a.adding_event_dttm, b2o.opening_event_dttm, bs.preview_event_dttm) AS source_event_ts_cet,

        COALESCE(a2o.product_id, o2a.product_id, b2o.product_id) AS product_id,
        COALESCE(a2o.product_name, o2a.product_name, b2o.product_name) AS product_name,
        COALESCE(a2o.pzn, o2a.pzn, b2o.pzn) AS pzn,

        bs.preview_event_id,
        b2o.opening_event_id,
        o2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    FROM banner_shows_to_source_screen AS bs

    -- join banner shows → product openings CTE
    LEFT JOIN banner_shows_to_openings AS b2o
        ON bs.preview_event_id = b2o.preview_event_id

    -- join opening → cart addings CTE
    LEFT JOIN openings_to_cart_addings AS o2a
        ON b2o.opening_event_id = o2a.opening_event_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS a2o
        ON o2a.adding_event_id = a2o.adding_event_id

    GROUP BY
        bs.event_dt,
        bs.source_screen,
        bs.block_name,
        COALESCE(a2o.order_created_dttm, o2a.adding_event_dttm, b2o.opening_event_dttm, bs.preview_event_dttm),

        COALESCE(a2o.product_id, o2a.product_id, b2o.product_id),
        COALESCE(a2o.product_name, o2a.product_name, b2o.product_name),
        COALESCE(a2o.pzn, o2a.pzn, b2o.pzn),

        bs.preview_event_id,
        b2o.opening_event_id,
        o2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    UNION ALL

    -- flat table for email events
    SELECT
        e2o.event_dt,
        'email' AS source,
        e2o.event_type AS first_page,
        COALESCE(a2o.pzn, o2a.pzn, e2o.pzn) AS placement,
        e2o.utm_campaign AS campaign_name,
        COALESCE(a2o.order_created_dttm, o2a.adding_event_dttm, e2o.opening_event_dttm, e2o.preview_event_dttm) AS source_event_ts_cet,

        COALESCE(a2o.product_id, o2a.product_id) AS product_id,
        COALESCE(a2o.product_name, o2a.product_name) AS product_name,
        COALESCE(a2o.pzn, o2a.pzn, e2o.pzn) AS pzn,

        e2o.opening_event_id AS preview_event_id,
        e2o.opening_event_id,
        o2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    FROM email_click_to_openings AS e2o

    -- join opening → cart addings CTE
    LEFT JOIN openings_to_cart_addings AS o2a
        ON e2o.opening_event_id = o2a.opening_event_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS a2o
        ON o2a.adding_event_id = a2o.adding_event_id

    GROUP BY
        e2o.event_dt,
        e2o.event_type,
        COALESCE(a2o.pzn, o2a.pzn, e2o.pzn),
        e2o.utm_campaign,
        COALESCE(a2o.order_created_dttm, o2a.adding_event_dttm, e2o.opening_event_dttm, e2o.preview_event_dttm),

        COALESCE(a2o.product_id, o2a.product_id),
        COALESCE(a2o.product_name, o2a.product_name),
        COALESCE(a2o.pzn, o2a.pzn, e2o.pzn),

        e2o.opening_event_id,
        e2o.opening_event_id,
        o2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    UNION ALL

    -- flat table for popup events
    SELECT
        pp.event_dt,
        'popup' AS source,
        pp.event_type AS first_page,
        pp.product_id AS placement,
        NULL AS campaign_name,
        COALESCE(a2o.order_created_dttm, p2a.adding_event_dttm, pp.opening_event_dttm) AS source_event_ts_cet,

        COALESCE(a2o.product_id, p2a.product_id, pp.alternative_product_id) AS product_id,
        COALESCE(a2o.product_name, p2a.product_name) AS product_name,
        COALESCE(a2o.pzn, p2a.pzn) AS pzn,

        pp.opening_event_id AS preview_event_id,
        pp.opening_event_id,
        p2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity

    FROM popups AS pp

    -- join popup → adding CTE
    LEFT JOIN popup_shows_to_addings AS p2a
        ON pp.opening_event_id = p2a.opening_event_id

    -- join cart → order CTE
    LEFT JOIN cart_addings_to_orders AS a2o
        ON p2a.adding_event_id = a2o.adding_event_id

    GROUP BY
        pp.event_dt,
        pp.event_type,
        pp.product_id,
        COALESCE(a2o.order_created_dttm, p2a.adding_event_dttm, pp.opening_event_dttm),

        COALESCE(a2o.product_id, p2a.product_id, pp.alternative_product_id),
        COALESCE(a2o.product_name, p2a.product_name),
        COALESCE(a2o.pzn, p2a.pzn),

        pp.opening_event_id,
        pp.opening_event_id,
        p2a.adding_event_id,
        a2o.order_id,
        a2o.order_products_price,
        a2o.order_quantity
),


--------------------------------------------------------------------------------------------------------------------------
-- preparing aggregated data with chosen maximal source – where user had a final interaction with the product
--------------------------------------------------------------------------------------------------------------------------
pre_final_agg_table AS (
    SELECT
        MAX(event_dt) AS event_dt,
        MAX_BY(source, source_event_ts_cet) AS source,
        MAX_BY(first_page, source_event_ts_cet) AS first_page,
        MAX_BY(placement, source_event_ts_cet) AS placement,
        MAX_BY(campaign_name, source_event_ts_cet) AS campaign_name,
        product_id,
        product_name,
        pzn,
        preview_event_id,
        COUNT(DISTINCT opening_event_id) AS openings,
        COUNT(DISTINCT adding_event_id) AS addings,
        order_id,
        order_products_price,
        order_quantity
    FROM pre_final_flat_table
    GROUP BY
        product_id,
        product_name,
        pzn,
        preview_event_id,
        order_id,
        order_products_price,
        order_quantity
)


SELECT
    event_dt,
    CAST(DATE_TRUNC('day', event_dt) AS DATE) AS event_date,
    source,
    first_page,
    placement,
    campaign_name,
    product_id,
    product_name,
    pzn,
    COUNT(DISTINCT preview_event_id) AS previews,
    SUM(openings) AS openings,
    SUM(addings) AS addings,
    order_id,
    order_products_price,
    order_quantity
FROM pre_final_agg_table
WHERE
    TRUE
    AND event_dt IS NOT NULL
    AND DATE(event_dt) < CURRENT_DATE()
GROUP BY
    event_dt,
    CAST(DATE_TRUNC('day', event_dt) AS DATE),
    source,
    first_page,
    placement,
    campaign_name,
    product_id,
    product_name,
    pzn,
    order_id,
    order_products_price,
    order_quantity

DISTRIBUTE BY event_date
