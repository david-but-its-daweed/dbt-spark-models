{{
  config(
    materialized='incremental',
    alias='products_with_target_price',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'false'
    },
  )
}}
--------------------------------------------------------------------------
-----------------------Forming list of JS products------------------------
--------------------------------------------------------------------------

WITH edlp_products AS (----------- a temporary filter: only for non edlp products  
    SELECT
        product_id,
        partition_date
    FROM {{ source('goods', 'product_labels') }}
    WHERE
        label = "EDLP"
    {% if is_incremental() %}
        AND partition_date >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 90 DAY
    {% else %}
        AND partition_date >= DATE("2024-02-18") - INTERVAL 90 DAY
    {% endif %}
),
------------------------------------------------------------------------------
------------------Forming the list of automatically selected products---------
------------------------------------------------------------------------------
filtered_products AS (
    SELECT
        m.product_id,
        m.partition_date,
        m.main_category,
        m.merchant_id,
        m.product_rating_60_days,
        m.gmv_60_days,
        m.orders_60_days,
        m.orders_with_nf_share_1_year,
        m.merchant_cancel_rate_1_year,
        m.merchant_price_index,
        c.days_orders_minimum_60 AS criteria_days_orders_minimum_60,
        c.days_gmv_minimum_60 AS criteria_days_gmv_minimum_60,
        c.merchant_cancel_rate_maximum AS criteria_merchant_cancel_rate_maximum,
        c.business_line,
        d.name AS merchant_name,
        1 AS reason_of_participation
    FROM {{ ref('initial_metrics_set') }} AS m
    LEFT JOIN {{ source('category_management', 'joom_select_manual_criteria') }} AS c ON m.main_category = c.category
    LEFT JOIN {{ source('mart', 'dim_merchant') }} AS d ON m.merchant_id = d.merchant_id
    LEFT JOIN {{ source('category_management', 'joom_select_product_black_list') }} AS bl ON m.product_id = bl.product_id
    LEFT JOIN edlp_products AS e
        ON
            m.product_id = e.product_id
            AND e.partition_date = m.partition_date - INTERVAL 1 DAY
    WHERE
        m.gmv_60_days >= c.days_gmv_minimum_60         --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.orders_60_days >= c.days_orders_minimum_60 --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.merchant_cancel_rate_1_year <= c.merchant_cancel_rate_maximum --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.orders_with_nf_share_1_year <= 0.05  -- this parameter from https://joom-team.atlassian.net/browse/AN-2985
        AND m.product_rating_60_days >= 4.3        -- this parameter from https://joom-team.atlassian.net/browse/AN-2985
        AND d.origin_name = "Chinese"              -- a temporary filter: only chinese origin            
        -- a temporary filter: exclude the edlp products
        AND e.product_id IS NULL
        AND bl.product_id IS NULL
    {% if is_incremental() %}
        AND m.partition_date >= DATE('{{ var("start_date_ymd") }}')
    {% else %}
        AND m.partition_date >= DATE("2024-02-19")
    {% endif %}
        
),
------------------------------------------------------------------------------
------------------Forming the list of manually selected products--------------
------------------------------------------------------------------------------
manual_products AS (
    SELECT
        product_id,
        MAX(discount) AS discount,
        0 AS reason_of_participation
    FROM {{ source('category_management', 'joom_select_product_white_list') }}
    GROUP BY 1
),

all_products AS (
    SELECT
        product_id,
        partition_date,
        main_category,
        merchant_id,
        business_line,
        merchant_name,
        MAX(product_rating_60_days) AS product_rating_60_days,
        MAX(gmv_60_days) AS gmv_60_days,
        MAX(orders_60_days) AS orders_60_days,
        MAX(orders_with_nf_share_1_year) AS orders_with_nf_share_1_year,
        MAX(merchant_cancel_rate_1_year) AS merchant_cancel_rate_1_year,
        MAX(merchant_price_index) AS merchant_price_index,
        MAX(criteria_days_orders_minimum_60) AS criteria_days_orders_minimum_60,
        MAX(criteria_days_gmv_minimum_60) AS criteria_days_gmv_minimum_60,
        MAX(criteria_merchant_cancel_rate_maximum) AS criteria_merchant_cancel_rate_maximum,
        MIN(reason_of_participation) AS reason_of_participation
    FROM (
        SELECT
            product_id,
            partition_date,
            main_category,
            merchant_id,
            product_rating_60_days,
            gmv_60_days,
            orders_60_days,
            orders_with_nf_share_1_year,
            merchant_cancel_rate_1_year,
            merchant_price_index,
            criteria_days_orders_minimum_60,
            criteria_days_gmv_minimum_60,
            criteria_merchant_cancel_rate_maximum,
            business_line,
            merchant_name,
            reason_of_participation
        FROM filtered_products
        UNION ALL
        SELECT
            p.product_id,
            DATE('{{ var("start_date_ymd") }}') AS partition_date,
            c.l1_merchant_category_name AS main_category,
            p.merchant_id,
            0 AS product_rating_60_days,
            0 AS gmv_60_days,
            0 AS orders_60_days,
            0 AS orders_with_nf_share_1_year,
            0 AS merchant_cancel_rate_1_year,
            0 AS merchant_price_index,
            0 AS criteria_days_orders_minimum_60,
            0 AS criteria_days_gmv_minimum_60,
            0 AS criteria_merchant_cancel_rate_maximum,
            c.business_line,
            dm.name AS merchant_name,
            0 AS reason_of_participation
        FROM {{ source('mart', 'dim_published_product_min') }} AS p
        INNER JOIN manual_products AS m ON p.product_id = m.product_id
        INNER JOIN {{ source('mart', 'dim_merchant') }} AS dm ON dm.merchant_id = p.merchant_id
        INNER JOIN {{ ref('gold_merchant_categories') }} AS c ON p.category_id = c.merchant_category_id
    )
    GROUP BY
        product_id,
        partition_date,
        main_category,
        merchant_id,
        business_line,
        merchant_name
),
--------------------------------------------------------------------------
-----------------------Forming target price------------------------
--------------------------------------------------------------------------    
---- we need to exclude periods, when proposal was approved
---- otherwise we will demand discount from JS price over and over again
approved_proposals_periods AS (
    SELECT DISTINCT
        product_id,
        status_effective_from,
        COALESCE(status_effective_to, "2999-01-01") AS status_effective_to
    FROM {{ ref('js_proposal_backend_status_history_raw') }}
    WHERE status = "approved"
),

calendar_dt AS (
    SELECT col AS partition_date
    FROM (
        SELECT EXPLODE(sequence(to_date(CURRENT_DATE() - INTERVAL 365 DAY), to_date(CURRENT_DATE()), interval 1 day))
    )
),

promo_calendar AS (
    SELECT
        partition_date,
        product_id,
        promo_start_time_utc,
        promo_end_time_utc,
        discount
    FROM calendar_dt AS c
    LEFT JOIN {{ source('mart', 'promotions') }} AS p
        ON partition_date >= promo_start_time_utc
        AND partition_date < promo_end_time_utc
    WHERE promo_start_time_utc >= CURRENT_DATE() - INTERVAL 365 DAY
),

promo_prods AS (
    SELECT
        partition_date,
        product_id,
        MAX(discount)/100 AS discount
    FROM promo_calendar
    GROUP BY 1, 2
),
--------------------------------------------------------------------------    
---- As far as dim_published_variant_with_merchant contains 
---- many lines for each variant with the same price,
---- we shoud define effective period of a particular price for each variant
variants_stg_ex1 AS (
    SELECT
        variant_id,
        product_id,
        price,
        currency,
        value_partition,
        MAX(current_price) AS current_price,
        MIN(effective_ts) AS effective_ts,
        MAX(next_effective_ts) AS next_effective_ts
    FROM (
        SELECT
            variant_id,
            product_id,
            price,
            currency,
            effective_ts,
            next_effective_ts,
            SUM(value_partition) over (PARTITION BY variant_id, product_id ORDER BY effective_ts ) AS value_partition,
            FIRST(price) OVER (PARTITION BY variant_id, product_id ORDER BY effective_ts DESC) AS current_price
        FROM (
            SELECT
                variant_id,
                product_id,
                price,
                currency,
                effective_ts AS effective_ts,
                next_effective_ts AS next_effective_ts,
                CASE WHEN IF(price_lag IS NULL OR price_lag!= price, effective_ts, NULL) IS NULL THEN 0 ELSE 1 END AS value_partition    
            FROM (
                SELECT
                    variant_id,
                    product_id,
                    price / 1000000 AS price,
                    currency,
                    effective_ts,
                    next_effective_ts,
                    LAG(p.price / 1000000) OVER (PARTITION BY variant_id, product_id ORDER BY next_effective_ts) AS price_lag
                FROM {{ source('mart', 'dim_published_variant_with_merchant') }} AS p
                )
            )
        )
    GROUP BY 1, 2, 3, 4, 5
),

currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_currency_rate') }}
),

variants_stg_ex2 AS (
    SELECT 
        v.effective_ts,
        v.next_effective_ts,
        v.product_id,
        v.variant_id,
        v.price,
        v.currency,
        v.current_price,
        SUM(v.price * COALESCE((1 - p.discount), 1)) AS sum_price_with_discount,
        COUNT(p.partition_date) AS days_with_promo
    FROM variants_stg_ex1 AS v
    LEFT JOIN promo_prods AS p
        ON
            v.product_id = p.product_id
            AND p.partition_date >= v.effective_ts
            AND p.partition_date < v.next_effective_ts
    GROUP BY
        v.effective_ts,
        v.next_effective_ts,
        v.product_id,
        v.variant_id,
        v.price,
        v.current_price,
        v.currency
),
------------------------------------------------------------------------------    
---- Then we should exclude price periods, when the product participated in JS 
---- Otherwise we will demand discount over and over again 
variants AS (
    SELECT
        variant_id,
        product_id,
        currency,
        price,
        current_price,
        ROUND(sum_price_with_discount / days_with_promo, 3) AS avg_price_with_discount,
        effective_ts
    FROM (
        SELECT
            p.variant_id,
            p.product_id,
            p.price,
            p.effective_ts,
            p.current_price,
            p.currency,
            SUM(sum_price_with_discount) OVER (PARTITION BY p.variant_id, p.product_id) AS sum_price_with_discount,  
            SUM(days_with_promo)  OVER (PARTITION BY p.variant_id, p.product_id) AS days_with_promo,
            MAX(p.effective_ts) OVER (PARTITION BY p.variant_id, p.product_id) AS max_effective_ts
        FROM variants_stg_ex2 AS p
        LEFT JOIN approved_proposals_periods AS ap
            ON
                p.product_id = ap.product_id
                AND
                    (
                        (DATE(p.effective_ts - INTERVAL 3 HOURS) = DATE(ap.status_effective_from) 
                        AND DATE(p.next_effective_ts - INTERVAL 3 HOURS) = DATE(ap.status_effective_to))
                    OR 
                        (DATE(p.effective_ts - INTERVAL 3 HOURS) = DATE(ap.status_effective_from) 
                        AND DATE(p.next_effective_ts - INTERVAL 3 HOURS) = '9999-12-31')
                    )
        WHERE p.next_effective_ts > "2024-01-01"
            AND ap.product_id IS NULL
    )
    WHERE effective_ts = max_effective_ts
),


------------------------------------------------------------------------------    
---------- Collecting info about vatiants and merchant prices in usd  without promo
------------------------------------------------------------------------------    
products_n_variants AS (
    SELECT
        pt.*,
        v.variant_id,
        v.price * c.rate AS last_not_js_price_usd,
        v.current_price * c.rate AS current_price_usd,
        v.avg_price_with_discount * c.rate AS avg_price_with_discount_usd
    FROM all_products AS pt
    INNER JOIN variants AS v ON pt.product_id = v.product_id
    LEFT JOIN currency_rates AS c
        ON
            v.currency = c.currency_code
            AND pt.partition_date >= c.effective_date
            AND pt.partition_date < c.next_effective_date
),
------------------------------------------------------------------------------    
---------- Collecting info about vatiants and prices in usd with promo -------
------------------------------------------------------------------------------   

prices AS (
    SELECT
        o.product_id,
        o.product_variant_id,
        o.order_date_msk,
        MIN(o.merchant_list_price / o.product_quantity) AS min_merchant_list_price,
        MIN(o.merchant_sale_price / o.product_quantity) AS min_merchant_sale_price
    FROM {{ ref('gold_orders') }} AS o
    LEFT JOIN approved_proposals_periods AS a
        ON
            a.product_id = o.product_id
            AND o.order_datetime_utc <= a.status_effective_to
            AND o.order_datetime_utc >= a.status_effective_from
    WHERE
        1 = 1
    {% if is_incremental() %}
        AND o.order_date_msk >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 360 DAY
    {% else %}
        AND o.order_date_msk >= DATE("2024-02-19") - INTERVAL 360 DAY
    {% endif %}    
        AND o.merchant_list_price > 0
        AND o.merchant_sale_price > 0
        AND a.product_id IS NULL
    GROUP BY 1, 2, 3
),
------------------------------------------------------------------------------    
----------------- Collecting all vatiant's info together ----------------
------------------------------------------------------------------------------  
products_n_variants_n_prices AS (
    SELECT
        v.product_id,
        v.partition_date,
        v.main_category,
        v.merchant_id,
        v.variant_id,
        v.business_line,
        v.merchant_name,
        v.reason_of_participation,
        MIN(v.product_rating_60_days) AS product_rating_60_days,
        MIN(v.gmv_60_days) AS gmv_60_days,
        MIN(v.orders_60_days) AS orders_60_days,
        MIN(v.orders_with_nf_share_1_year) AS orders_with_nf_share_1_year,
        MIN(v.merchant_cancel_rate_1_year) AS merchant_cancel_rate_1_year,
        MIN(v.merchant_price_index) AS merchant_price_index,
        MIN(v.criteria_days_orders_minimum_60) AS criteria_days_orders_minimum_60,
        MIN(v.criteria_days_gmv_minimum_60) AS criteria_days_gmv_minimum_60,
        MIN(v.criteria_merchant_cancel_rate_maximum) AS criteria_merchant_cancel_rate_maximum,
        MIN(v.current_price_usd) AS current_price_usd,
        MIN(v.last_not_js_price_usd) AS last_not_js_price_usd,
        MIN(p.min_merchant_list_price) AS min_merchant_list_price,
        MIN(p.min_merchant_sale_price) AS min_merchant_sale_price,
        MIN(v.avg_price_with_discount_usd) AS avg_price_with_discount_usd,
        MIN(COALESCE(m.discount, d.rate)) AS discount_rate -- The rate of a discount from the handbook https://docs.google.com/spreadsheets/d/1JXqKXvYhJcaZWf69e1G5EXB0sZady_EEM6pfeGk6JHU/edit#gid=0 
                                                 -- or https://docs.google.com/spreadsheets/d/1fJpQl_JwumbWikePJkqkh3lhGXdmXf6B39VXmrFT1GI/edit#gid=0
    FROM products_n_variants AS v
    LEFT JOIN prices AS p
        ON
            v.product_id = p.product_id
            AND v.variant_id = p.product_variant_id
            AND v.partition_date >= p.order_date_msk
            AND p.order_date_msk >= v.partition_date - INTERVAL 90 DAY
    LEFT JOIN {{ source('category_management', 'js_merchant_discount_rate') }} AS d ON v.merchant_id = d.merchant_id    
    LEFT JOIN manual_products AS m ON p.product_id = m.product_id
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
------------------------------------------------------------------------------    
----------------- Forming target price for each variant ----------------
------------------------------------------------------------------------------  
target_price_stg AS (
    SELECT
        *,
        ROUND(current_price_usd / merchant_price_index, 3) AS merchant_price_index_price,
        CASE
            WHEN ---- just taking discount rate from manual table and apply to the last not JS price - manual way of calculation
                discount_rate IS NOT NULL
                THEN last_not_js_price_usd * discount_rate
            WHEN ---- when we have no information about sales or price index, we calculate price for variant as an average price for other variants of the product
                min_merchant_list_price IS NULL
                AND min_merchant_sale_price IS NULL
                AND ROUND(current_price_usd / merchant_price_index, 3) IS NULL
                AND current_price_usd >= avg_price_with_discount_usd
                THEN avg_price_with_discount_usd
            WHEN ---- when last not JS price still is the minimal price for the variant we take it with the discount as the target price
                last_not_js_price_usd < COALESCE(min_merchant_list_price, 1000000000) 
                AND last_not_js_price_usd < COALESCE(min_merchant_sale_price, 1000000000) 
                AND last_not_js_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                AND last_not_js_price_usd * COALESCE(discount_rate, 0.95) < current_price_usd
                THEN last_not_js_price_usd * COALESCE(discount_rate, 0.95)
            WHEN ---- when current price is the minimal price for the variant we take it without the discount as the target price
                current_price_usd < COALESCE(min_merchant_list_price, 1000000000) 
                AND current_price_usd < COALESCE(min_merchant_sale_price, 1000000000) 
                AND current_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN current_price_usd
            WHEN ---- when merchant list price in order is the minimal price for the variant we take it without the discount as the target price
                min_merchant_list_price <= COALESCE(min_merchant_sale_price, 1000000000)
                AND min_merchant_list_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN min_merchant_list_price
            WHEN ---- when merchant sale price in order is the minimal price for the variant we take it without the discount as the target price
                min_merchant_sale_price <= COALESCE(min_merchant_list_price, 1000000000)
                AND min_merchant_sale_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN min_merchant_sale_price
            WHEN ---- when pi list price in order is the minimal price for the variant we take it without the discount as the target price
                ROUND(current_price_usd / merchant_price_index, 3) <= COALESCE(min_merchant_list_price, 1000000000)
                AND ROUND(current_price_usd / merchant_price_index, 3) <= COALESCE(min_merchant_sale_price, 1000000000)
                THEN ROUND(current_price_usd / merchant_price_index, 3)
        END AS target_price_stg,     -- take minimum price current_price_usd, among min_merchant_list_price, min_merchant_sale_price, price_index_price, last_not_js_price_usd
        CASE
            WHEN
                discount_rate IS NOT NULL
                THEN "manual_discount"
            WHEN
                min_merchant_list_price IS NULL
                AND min_merchant_sale_price IS NULL
                AND ROUND(current_price_usd / merchant_price_index, 3) IS NULL
                AND current_price_usd >= avg_price_with_discount_usd
                THEN "avg_promo_price_discount"
            WHEN
                last_not_js_price_usd < COALESCE(min_merchant_list_price, 1000000000) 
                AND last_not_js_price_usd < COALESCE(min_merchant_sale_price, 1000000000) 
                AND last_not_js_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                AND last_not_js_price_usd * COALESCE(discount_rate, 0.95) < current_price_usd
                THEN "last_not_js_price_usd_with_discount"
            WHEN
                current_price_usd < COALESCE(min_merchant_list_price, 1000000000) 
                AND current_price_usd < COALESCE(min_merchant_sale_price, 1000000000) 
                AND current_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "current_price_usd"
            WHEN
                min_merchant_list_price <= COALESCE(min_merchant_sale_price, 1000000000)
                AND min_merchant_list_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "min_merchant_list_price"
            WHEN
                min_merchant_sale_price <= COALESCE(min_merchant_list_price, 1000000000)
                AND min_merchant_sale_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "min_merchant_sale_price"
            WHEN
                ROUND(current_price_usd / merchant_price_index, 3) <= COALESCE(min_merchant_list_price, 1000000000)
                AND ROUND(current_price_usd / merchant_price_index, 3) <= COALESCE(min_merchant_sale_price, 1000000000)
                THEN "price_index"
        END AS target_price_reason
    FROM products_n_variants_n_prices
)

SELECT
    SHA(CONCAT(product_id, partition_date)) AS proposal_id,
    product_id,
    partition_date,
    main_category,
    merchant_id,
    variant_id,
    business_line,
    merchant_name,
    CASE
        WHEN reason_of_participation = 1 THEN "automatically"
        WHEN reason_of_participation = 0 THEN "manually"
    END AS reason_of_participation,
    product_rating_60_days,
    gmv_60_days,
    orders_60_days,
    orders_with_nf_share_1_year,
    merchant_cancel_rate_1_year,
    merchant_price_index,
    criteria_days_orders_minimum_60,
    criteria_days_gmv_minimum_60,
    criteria_merchant_cancel_rate_maximum,
    current_price_usd,
    min_merchant_list_price,
    min_merchant_sale_price,
    merchant_price_index_price,
    avg_price_with_discount_usd,
    COALESCE(target_price_reason, "avg_other_variants_discount") AS target_price_reason,
    ROUND(AVG(target_price_stg / current_price_usd) OVER (PARTITION BY partition_date, product_id), 2) AS avg_product_discount,
    FLOOR(COALESCE(target_price_stg), 2) AS target_price
FROM target_price_stg