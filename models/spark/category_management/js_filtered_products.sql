{{
  config(
    materialized='incremental',
    alias='js_filtered_products',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@vasiukova_mn',
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
    LEFT JOIN {{ source('category_management', 'joom_select_manual_criteria') }} AS c ON m.main_category = c.category -- from https://docs.google.com/spreadsheets/d/1QSlcEnxAEHmoOMlcSV7fAxZl0pwYyDSOm1tSEvXVOZU/edit#gid=0
    LEFT JOIN {{ source('mart', 'dim_merchant') }} AS d ON m.merchant_id = d.merchant_id
    LEFT JOIN {{ source('category_management', 'joom_select_product_black_list') }} AS bl ON m.product_id = bl.product_id -- from https://docs.google.com/spreadsheets/d/12XvU3LAscRKKF4iFAIn_eNvQFOvxLUh_miv4B_RkMZQ/edit?gid=0#gid=0
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
        AND ( d.origin_name = "Chinese"            -- a temporary filter: only chinese origin  or merchant_id first 24 rows from list from https://joom-team.atlassian.net/browse/AN-3379 
            OR  m.merchant_id IN ("65eea31a238708eb4ee3502c","65cc35d0144e748709e72ae8","65cacb034ef29de4eaf1ea8f","657168bc0c3be88fcfde094b","6493ff6a397ba7d04e97a35d","641bd999a2e4f03234e7f523",
                                "62da5b66797cacb687948fd4","62b33993a73a5a0b96e06ff6","62b02810141f0618e9a0d90a","62a054c5fc6d8bd6d9e2f338","62170618dfe1c46ecd689af1","6156a5e939f22972c5121356",
                                "60f5248425c01ec5873c4bbd","5e384a0406d9540b010fa4d0","5cf4ddb18b45130b017c0e16","5ce7faff8b45130b018601bf","5cdedea78b2c370b0162f3a4","62f35104abb496a02e7359b6"
                                "6499251b7a76a242e05ee08d","63849ba073c6932a64ef7288","5f29031ca244430b065022fa","61c989872345d2754cc8727e","61fb7080f4be39f12f27deb8","633ecff8a3d631358b111362")
            )           
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
        0 AS reason_of_participation -- 0 for manual, 1 for automatic selection
    FROM {{ source('category_management', 'joom_select_product_white_list') }} -- https://docs.google.com/spreadsheets/d/1fJpQl_JwumbWikePJkqkh3lhGXdmXf6B39VXmrFT1GI/edit?gid=0#gid=0
    GROUP BY 1
)

--------------------------------------------------------------------------------------
--------------- Merging the manual list and automatically selected products ----------
--------------------------------------------------------------------------------------

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