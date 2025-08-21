{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='new_pp_prices_history',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@vasiukova_mn',
        'bigquery_load': 'true'
    }
  )
}}

WITH kam_list AS (
    SELECT
        merchant_id,
        SPLIT(ANY_VALUE(kam_email), '@')[0] AS kam
    FROM {{ source('category_management', 'merchant_kam_materialized') }} AS tt
    WHERE
        tt.quarter = (SELECT MAX(t.quarter) FROM {{ source('category_management', 'merchant_kam_materialized') }} AS t)
        AND kam_email IS NOT NULL
    GROUP BY 1
),

targets AS (
    SELECT
        product_id,
        variant_id,
        average_sales_price AS target_price
    FROM {{ source('merchant', 'product_variant_prices_prepared_for_cassandra') }}
    WHERE partition_date = '2025-08-12' -- тут должно быть зафиксировано, потому что окно не скользящее
),

previews AS (
    SELECT
        partition_date,
        product_id,
        SUM(preview_count) AS sum_previews
    FROM {{ source('price', 'preview_prices') }}
    {% if is_incremental() %}
        WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
    {% else %}
        WHERE partition_date >= '2025-04-01'
    {% endif %}
    GROUP BY 1, 2
    HAVING SUM(preview_count) > 10
),

prices AS (
    SELECT
        effective_ts,
        next_effective_ts,
        product_id,
        variant_id,
        price / 1e6 AS price
    FROM {{ source('mart', 'dim_published_variant_with_merchant') }}
    WHERE effective_ts > '2024-01-01'
),

orders AS (
    SELECT
        product_id,
        SUM(gmv_initial) AS gmv
    FROM {{ ref('gold_orders') }}
    WHERE
        order_date_msk >= DATE_SUB(DATE('{{ var("start_date_ymd") }}'), 30)
        AND (NOT is_refunded OR refund_reason NOT IN ('cancelled_by_merchant', 'fraud'))
    GROUP BY 1
),

price_history AS (
    SELECT
        w.partition_date,
        w.product_id,
        ANY_VALUE(w.sum_previews) AS sum_previews,
        AVG(IF(i.price < 0.01, NULL, (i.price - t.target_price) / t.target_price)) AS price_change,
        MIN(i.price) AS min_price,
        MIN_BY(t.target_price, i.price) AS benchmark_price
    FROM previews AS w
    INNER JOIN prices AS i
        ON
            w.product_id = i.product_id
            AND w.partition_date >= DATE(i.effective_ts)
            AND w.partition_date < DATE(i.next_effective_ts)
    INNER JOIN targets AS t
        ON
            w.product_id = t.product_id
            AND i.variant_id = t.variant_id
    GROUP BY 1, 2
)

SELECT
    ph.partition_date,
    ph.product_id,
    gm.merchant_id,
    gm.merchant_name,
    kl.kam,
    gm.origin_name,
    gp.l1_merchant_category_name,
    gp.l2_merchant_category_name,
    COALESCE(o.gmv, 0) AS gmv,
    ph.price_change,
    ph.sum_previews,
    ph.min_price,
    ph.benchmark_price
FROM price_history AS ph
INNER JOIN {{ ref('gold_products') }} AS gp
    ON ph.product_id = gp.product_id
LEFT JOIN {{ ref('gold_merchants') }} AS gm
    ON gp.merchant_id = gm.merchant_id
LEFT JOIN orders AS o
    ON ph.product_id = o.product_id
LEFT JOIN kam_list AS kl
    ON gm.merchant_id = kl.merchant_id