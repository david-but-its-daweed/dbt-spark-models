{{
  config(
    enabled=false,
    materialized='incremental',
    alias='products_with_target_price',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'false'
    },
  )
}}

--------------------------------------------------------------------------
-----------------------Forming target price------------------------
--------------------------------------------------------------------------    
---- we need to exclude periods, when proposal was approved
---- otherwise we will demand discount from JS price over and over again
WITH all_products AS (
    SELECT
        *
    FROM {{ ref('js_filtered_products') }}
    WHERE 
        {% if is_incremental() %}
            partition_date >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 90 DAY
        {% else %}
            partition_date >= DATE("2024-02-18") - INTERVAL 90 DAY
        {% endif %}
),
        
------------------------------------------------------------------------------
------------------Forming the list of manually selected products--------------
------------------------------------------------------------------------------
manual_products AS (
    SELECT
        product_id,
        MAX(discount) AS discount,
        0 AS reason_of_participation -- 0 for manual, 1 for automatic selection
    FROM {{ source('category_management', 'joom_select_product_white_list') }} -- https://docs.google.com/spreadsheets/d/1fJpQl_JwumbWikePJkqkh3lhGXdmXf6B39VXmrFT1GI/edit?gid=0#gid=0
    GROUP BY 1
),

approved_proposals_periods AS (
    SELECT DISTINCT
        product_id,
        status_effective_from,
        COALESCE(status_effective_to, "2999-01-01") AS status_effective_to
    FROM {{ ref('js_proposal_backend_status_history_raw') }}
    WHERE status = "approved"
),

--- As far as dim_published_variant_with_merchant contains info about price in a merchant currency, we need to convert it into usd for comparing with other prices.
--- So, here we can find currency rate for each day for any currency. 
currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_currency_rate') }}
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
        ROUND(sum_price_with_discount / days_with_promo, 3) AS avg_price_with_discount, -- defining promo avg promo price
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
        FROM {{ ref('js_prices_log') }} AS p
        LEFT JOIN approved_proposals_periods AS ap
            ON
                p.product_id = ap.product_id
                AND
                    (
                        (DATE(p.effective_ts - INTERVAL 3 HOURS) >= DATE(ap.status_effective_from) 
                        AND DATE(p.next_effective_ts - INTERVAL 3 HOURS) <= DATE(ap.status_effective_to))
                    OR 
                        (DATE(p.effective_ts - INTERVAL 3 HOURS) >= DATE(ap.status_effective_from) 
                        AND DATE(p.next_effective_ts - INTERVAL 3 HOURS) <= '9999-12-31')
                    )
        WHERE p.next_effective_ts > "2024-01-01"
            AND ap.product_id IS NULL
    )
    WHERE effective_ts = max_effective_ts --- we need only the most recent period after js participation filtering. So, current_price - is the last actual price without JS participation
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
---------- Collecting info about variants and prices in usd from orders ------
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
----------------- Collecting all variant's info together ----------------
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
    LEFT JOIN manual_products AS m ON v.product_id = m.product_id
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
    ROUND(AVG(target_price_stg / current_price_usd) OVER (PARTITION BY partition_date, product_id), 2) AS avg_product_discount, -- the relic from the previous algorithm iteration, can be deleted
    FLOOR(COALESCE(target_price_stg), 2) AS target_price
FROM target_price_stg