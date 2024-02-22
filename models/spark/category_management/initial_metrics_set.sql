{{
  config(
    materialized='incremental',
    alias='initial_metrics_set',
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

WITH price_index_stg_1 AS (
    SELECT
        a.joom_product_id AS product_id,
        d.merchant_id,
        d.l1_merchant_category_name,
        a.joom_merchant_price_usd / a.aliexpress_merchant_price_usd AS full_price_joom_to_aliexpress_rate
    FROM  {{ source('mi_analytics', 'aliexpress_joom_price_index') }} AS a
    LEFT JOIN {{ ref('gold_products') }} AS d ON a.joom_product_id = d.product_id
    WHERE
        a.aliexpress_merchant_price_usd IS NOT NULL
        AND a.aliexpress_merchant_price_usd >= 0
    {% if is_incremental() %}
        AND a.partition_date = DATE('{{ var("start_date_ymd") }}') - INTERVAL 1 DAY
    {% endif %}
        AND a.partition_date >= DATE("2024-02-19") - INTERVAL 1 DAY 
),

price_index_stg_2 AS (
    SELECT
        product_id,
        l1_merchant_category_name,
        merchant_id,
        PERCENTILE(full_price_joom_to_aliexpress_rate, 0.5) AS merchant_price_index
    FROM price_index_stg_1 AS r
    WHERE r.full_price_joom_to_aliexpress_rate IS NOT NULL
    GROUP BY 1, 2, 3
),

price_index_final AS (
    SELECT DISTINCT
        l1_merchant_category_name,
        product_id,
        merchant_id,
        PERCENTILE(merchant_price_index, 0.5) OVER (PARTITION BY l1_merchant_category_name, merchant_id) AS merchant_price_index
    FROM price_index_stg_2
),

merchant_cancel_rate AS (
    SELECT
        merchant_id,
        merchant_cancel_rate_1y AS merchant_cancel_rate_1_year
    FROM {{ source('merchant', 'merchant_performance') }}
    WHERE
        partition_date >= DATE("2024-02-19") 
    {% if is_incremental() %}
       AND partition_date = DATE('{{ var("start_date_ymd") }}')  - INTERVAL 1 DAY 
    {% endif %}
         
),

orders_stats_60_days AS (
    SELECT
        o.product_id,
        o.merchant_id,
        LAST(c.l1_merchant_category_name) AS main_category,
        SUM(o.gmv_initial) AS gmv_60_days,
        COUNT(o.order_id) AS orders_60_days,
        ROUND(AVG(o.product_rating), 2) AS product_rating_60_days
    FROM {{ ref('gold_orders') }} AS o
    INNER JOIN {{ ref('gold_merchant_categories') }} AS c ON o.merchant_category_id = c.merchant_category_id
    WHERE
        NOT (o.refund_reason IN ('fraud', 'cancelled_by_customer') AND o.refund_reason IS NOT NULL)
    {% if is_incremental() %}
        AND o.order_date_msk <= DATE('{{ var("start_date_ymd") }}') - INTERVAL 1 DAY
        AND o.order_date_msk >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 61 DAY
    {% endif %}
        AND o.order_date_msk >= DATE("2024-02-19") - INTERVAL 61 DAY
        AND o.order_date_msk <= DATE(CURRENT_DATE()) - INTERVAL 1 DAY
    GROUP BY 1, 2
),

orders_negative_feedback_stats AS (
    SELECT
        o.product_id,
        COUNT(o.order_id) AS orders,
        ROUND(COUNT(IF(o.is_negative_feedback = 1, o.order_id, NULL)) / COUNT(o.order_id), 3) AS orders_with_nf_share_360_days
    FROM {{ ref('gold_orders') }} AS o
    WHERE
        NOT (o.refund_reason IN ('fraud', 'cancelled_by_customer') AND o.refund_reason IS NOT NULL)
    {% if is_incremental() %}
        AND order_date_msk <= DATE('{{ var("start_date_ymd") }}') - INTERVAL 91 DAY
        AND order_date_msk >= DATE('{{ var("start_date_ymd") }}') - INTERVAL 360 DAY
    {% endif %}
        AND o.order_date_msk <= DATE(CURRENT_DATE()) - INTERVAL 91 DAY
        AND o.order_date_msk >= DATE("2024-02-19") - INTERVAL 360 DAY
    GROUP BY 1
)

    SELECT 
        DATE('{{ var("start_date_ymd") }}') AS partition_date,
        o.product_id,
        o.merchant_id,
        o.main_category,
        o.gmv_60_days AS gmv_60_days,   
        o.orders_60_days AS orders_60_days,    
        ROUND(o.product_rating_60_days, 3) AS product_rating_60_days,
        COALESCE(orders_with_nf_share_360_days, 0) AS orders_with_nf_share_1_year,
        ROUND(merchant_cancel_rate_1_year, 4) AS merchant_cancel_rate_1_year,
        ROUND(merchant_price_index, 3) AS merchant_price_index
    FROM orders_stats_60_days AS o
    LEFT JOIN orders_negative_feedback_stats AS n ON o.product_id = n.product_id
    LEFT JOIN merchant_cancel_rate AS c ON o.merchant_id = c.merchant_id
    LEFT JOIN price_index_final AS i ON i.product_id = o.product_id



