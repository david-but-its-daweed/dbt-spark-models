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
    FROM goods.product_labels
    WHERE
        label = "EDLP"
        AND partition_date >= CURRENT_DATE() - INTERVAL 90 DAY
),

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
        "automatically" AS reason_of_participation
    FROM category_management.initial_metrics_set AS m
    LEFT JOIN category_management.joom_select_manual_criteria AS c ON m.main_category = c.category
    LEFT JOIN mart.dim_merchant AS d ON m.merchant_id = d.merchant_id
    LEFT JOIN edlp_products AS e
        ON
            m.product_id = e.product_id
            AND e.partition_date = m.partition_date - INTERVAL 1 DAY
    INNER JOIN ekutynina.js_technicallaunch_products AS tl -- filter only for tecnical launch
        ON
            m.product_id = tl.product_id
            AND m.merchant_id = tl.merchant_id
    WHERE
        m.gmv_60_days >= c.days_gmv_minimum_60         --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.orders_60_days >= c.days_orders_minimum_60 --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.merchant_cancel_rate_1_year <= c.merchant_cancel_rate_maximum --from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
        AND m.orders_with_nf_share_1_year <= 0.05  -- this parameter from https://joom-team.atlassian.net/browse/AN-2985
        AND m.product_rating_60_days >= 4.3        -- this parameter from https://joom-team.atlassian.net/browse/AN-2985
        AND d.origin_name = "Chinese"              -- a temporary filter: only chinese origin            
        -- a filter for the first launch
        AND m.merchant_id IN (
            "5effe5ebc24e2f0b0623e77e", "621e26f5dc27724f0bc46c1e", "1510770926014486775-107-11-709-2088178538", "60b788c5e68365c96bf9e35a",
            "5cb6f20d6ecda80b01b39608", "1482223842694985483-168-11-26312-1078175402", "1481623505174558053-240-11-629-1881556930", "1515013786717211647-27-11-26193-3983838569",
            "5d1586788b45130b0122226f", "60cc9cff3ab7320b9fcfc134", "5c48202e8b45130b015b4e30", "5ba7a16c8b2c370bcc0cc0c1", "5c3c0a1f8b45130b01d28acb", "62c55fc41649da5a212ed30f",
            "1510945977073183125-13-11-26193-588585207", "5ad9c38a8b45130b59fa77ae", "1521655194930946012-6-11-39131-1938131666", "1520647475183396773-20-11-709-4164857933",
            "6103aaa6d527af18e5ae7aa2", "617bbe56ebf6a381d074e7b7"
        )
        AND e.product_id IS NULL
    {% if is_incremental() %}
        AND m.partition_date >= DATE('{{ var("start_date_ymd") }}')
    {% endif %}
),
--------------------------------------------------------------------------
-----------------------Forming target price------------------------
--------------------------------------------------------------------------    

variants AS (
    SELECT
        variant_id,
        product_id,
        price / 1000000 AS price,
        currency
    FROM mart.dim_published_variant_with_merchant
    WHERE next_effective_ts > "9999-12-31"
),

currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM mart.dim_currency_rate
),

products_n_variants AS (
    SELECT
        pt.*,
        v.variant_id,
        v.price * c.rate AS current_price_usd
    FROM filtered_products AS pt
    INNER JOIN variants AS v ON pt.product_id = v.product_id
    LEFT JOIN currency_rates AS c
        ON
            v.currency = c.currency_code
            AND pt.partition_date >= c.effective_date
            AND pt.partition_date < c.next_effective_date
),

prices AS (
    SELECT
        product_id,
        product_variant_id,
        order_date_msk,
        MIN(merchant_list_price / product_quantity) AS min_merchant_list_price,
        MIN(merchant_sale_price / product_quantity) AS min_merchant_sale_price
    FROM gold.orders
    WHERE
        NOT (refund_reason IN ("fraud", "cancelled_by_customer") AND refund_reason IS NOT NULL)
        AND order_date_msk >= CURRENT_DATE() - INTERVAL 360 DAY
        AND merchant_list_price > 0
        AND merchant_sale_price > 0
    GROUP BY 1, 2, 3
),

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
        MAX(v.merchant_cancel_rate_1_year) AS merchant_cancel_rate_1_year1,
        MIN(v.merchant_price_index) AS merchant_price_index,
        MIN(v.criteria_days_orders_minimum_60) AS criteria_days_orders_minimum_60,
        MIN(v.criteria_days_gmv_minimum_60) AS criteria_days_gmv_minimum_60,
        MIN(v.criteria_merchant_cancel_rate_maximum) AS criteria_merchant_cancel_rate_maximum,
        MIN(v.current_price_usd) AS current_price_usd,
        MIN(p.min_merchant_list_price) AS min_merchant_list_price,
        MIN(p.min_merchant_sale_price) AS min_merchant_sale_price
    FROM products_n_variants AS v
    LEFT JOIN prices AS p
        ON
            v.product_id = p.product_id
            AND v.variant_id = p.product_variant_id
            AND v.partition_date >= p.order_date_msk
            AND p.order_date_msk >= v.partition_date - INTERVAL 90 DAY
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

target_price_stg AS (
    SELECT
        *,
        ROUND(current_price_usd / merchant_price_index, 3) AS merchant_price_index_price,
        CASE
            WHEN
                current_price_usd < min_merchant_list_price
                AND current_price_usd < min_merchant_sale_price
                AND current_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN current_price_usd * 0.95
            WHEN
                min_merchant_list_price <= min_merchant_sale_price
                AND min_merchant_list_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN min_merchant_list_price * 0.95
            WHEN
                min_merchant_sale_price <= min_merchant_list_price
                AND min_merchant_sale_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN min_merchant_sale_price * 0.95
            WHEN
                COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000) <= min_merchant_list_price
                AND COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000) <= min_merchant_sale_price
                THEN ROUND(current_price_usd / merchant_price_index, 3) * 0.95
        END AS target_price_stg,     -- take minimum price current_price_usd, among min_merchant_list_price, min_merchant_sale_price, price_index_price
        CASE
            WHEN
                current_price_usd < min_merchant_list_price
                AND current_price_usd < min_merchant_sale_price
                AND current_price_usd < COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "current_price_usd"
            WHEN
                min_merchant_list_price <= min_merchant_sale_price
                AND min_merchant_list_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "min_merchant_list_price"
            WHEN
                min_merchant_sale_price <= min_merchant_list_price
                AND min_merchant_sale_price <= COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000)
                THEN "min_merchant_sale_price"
            WHEN
                COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000) <= min_merchant_list_price
                AND COALESCE(ROUND(current_price_usd / merchant_price_index, 3), 1000000000) <= min_merchant_sale_price
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
    reason_of_participation,
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
    target_price_reason,
    ROUND(AVG(target_price_stg / current_price_usd) OVER (PARTITION BY partition_date, product_id), 2) AS avg_product_discount,
    -- forming target price: if variant's min_merchant_list_price, min_merchant_sale_price, price_index_price are nulls
    -- we take average discount for other variants of this products, where at list one of  min_merchant_list_price, min_merchant_sale_price, price_index_price are not null. 
    FLOOR(COALESCE(
        target_price_stg, current_price_usd * (AVG(target_price_stg / current_price_usd) OVER (PARTITION BY partition_date, product_id))
    ), 2) AS target_price
FROM target_price_stg