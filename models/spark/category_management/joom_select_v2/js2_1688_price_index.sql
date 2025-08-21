{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='js2_1688_price_index',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@vasiukova_mn',
        'bigquery_load': 'true'
    },
  )
}}

WITH price_index_raw AS (
    SELECT
        partition_date,
        joom_product_id,
        joom_variant_id,
        ROUND(joom_merchant_price, 2) AS merchant_price,
        ROUND(ali1688_min_b2b_price, 2) AS 1688_min_price,
        ROUND(ali1688_median_b2b_price, 2) AS 1688_median_price,
        ROUND(ali1688_target_price, 2) AS 1688_target_price,
        ROUND(merchant_price_joom_to_1688_target_price_rate, 3) AS price_index,
        LAG(partition_date) OVER (PARTITION BY joom_product_id, joom_variant_id ORDER BY partition_date) AS last_price_dt,
        LAG(ali1688_target_price) OVER (PARTITION BY joom_product_id, joom_variant_id ORDER BY partition_date) AS last_price,
        ARRAY_AGG((ali1688_target_price, partition_date)) OVER (PARTITION BY joom_product_id, joom_variant_id) AS all_prices_per_variant
    FROM {{ source('mi_analytics', 'ali1688_joom_target_prices') }}
    {% if is_incremental() %}
        WHERE partition_date BETWEEN DATE('{{ var("start_date_ymd") }}') - 6 AND DATE('{{ var("start_date_ymd") }}')
    {% endif %}
),

price_index AS (
    SELECT
        partition_date,
        joom_product_id AS product_id,
        joom_variant_id AS variant_id,
        merchant_price,
        1688_min_price,
        1688_median_price,
        1688_target_price,
        price_index,
        COUNT(joom_variant_id) OVER (PARTITION BY joom_product_id, partition_date) AS parsed_variants_per_product,
        ARRAY_MIN(FILTER(all_prices_per_variant, x -> x.partition_date BETWEEN partition_date - 6 AND partition_date - 1).ali1688_target_price) AS min_target_price_over_week,
        ARRAY_MAX(FILTER(all_prices_per_variant, x -> x.partition_date BETWEEN partition_date - 6 AND partition_date - 1).ali1688_target_price) AS max_target_price_over_week,
        CASE 
            -- новый, если за неделю не было ни одной строки
            WHEN last_price_dt IS NULL OR last_price_dt < partition_date - 7 THEN 'new'
            -- не новый, предыдущая цена примерно такая же и изменение за неделю незначительное
            WHEN last_price_dt = partition_date - 1 
                AND 1688_target_price / last_price BETWEEN 0.95 AND 1.05
                AND 1688_target_price BETWEEN 0.95 * min_target_price_over_week and 1.05 * max_target_price_over_week
                THEN 'same_price'
            -- не новый, но вчера цены не было или вчера была другая цена
            WHEN (last_price_dt != partition_date - 1 or (last_price_dt = partition_date - 1 AND last_price!= 1688_target_price))
                AND NOT 1688_target_price BETWEEN 0.95 * min_target_price_over_week AND 1.05 * max_target_price_over_week
                THEN 'price_changed' 
            -- не новый, но вчера цены не было или вчера была другая цена
            ELSE 'price_changed' 
        END AS product_pi_status
    FROM price_index_raw
    {% if is_incremental() %}
        WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
    {% endif %}
),

pi_products AS (
    SELECT DISTINCT
        product_id,
        variant_id
    FROM price_index
),

calendar AS (
    SELECT
        t.partition_date,
        p.product_id,
        p.variant_id
    {% if is_incremental() %}
        FROM (SELECT EXPLODE(SEQUENCE(DATE('{{ var("start_date_ymd") }}') - 90, DATE('{{ var("start_date_ymd") }}'), INTERVAL 1 DAY)) AS partition_date) AS t
    {% else %}
        FROM (SELECT EXPLODE(SEQUENCE(DATE('2025-03-20') - 90, DATE('{{ var("start_date_ymd") }}'), INTERVAL 1 DAY)) AS partition_date) AS t
    {% endif %}
    CROSS JOIN pi_products AS p
),

kams AS (
    SELECT
        merchant_id,
        quarter AS partition_quarter,
        REPLACE(MAX_BY(kam_email, import_time), '@joom.com', '') AS kams
    FROM {{ source('category_management', 'merchant_kam_materialized') }}
    WHERE merchant_id IS NOT NULL
    GROUP BY 1, 2
),

labels AS (
    SELECT
        partition_date,
        product_id,
        COUNT_IF(label = 'joom_select') > 0 AS is_js,
        COUNT_IF(label = 'fbj_more_1d_stock') > 0 AS is_fbj
    FROM {{ source('goods', 'product_labels') }}
    {% if is_incremental() %}
        WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
        {% else %}
        WHERE partition_date >= '2025-03-20'
        {% endif %}
        AND product_id IN (SELECT DISTINCT p.product_id FROM price_index AS p)
        AND label IN ('joom_select', 'fbj_more_1d_stock')
    GROUP BY 1, 2
),

-- заказы не стала делить на ру / нонру, так что кажется, там будет аналогично показам
-- здесь взяла за 30 дней, потмоу то товары могут получать мало заказов + витрина не очень большая
orders_raw AS (
    SELECT
        order_date_msk AS partition_date,
        product_id,
        product_variant_id AS variant_id,
        COUNT(order_id) AS orders_1d,
        SUM(gmv_initial) AS gmv_1d
    FROM {{ ref('gold_orders') }}
    {% if is_incremental() %}
        WHERE order_date_msk BETWEEN DATE('{{ var("start_date_ymd") }}') - 90 AND DATE('{{ var("start_date_ymd") }}')
    {% else %}
        WHERE order_date_msk >= DATE('2025-03-20') - 90
    {% endif %}
    GROUP BY 1, 2, 3
),

all_orders AS (
    SELECT
        *,
        SUM(all_orders_1d) OVER (ORDER BY partition_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS all_orders_30d,
        SUM(all_gmv_1d) OVER (ORDER BY partition_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS all_gmv_30d
    FROM (
        SELECT
            partition_date,
            SUM(orders_1d) AS all_orders_1d,
            SUM(gmv_1d) AS all_gmv_1d
        FROM orders_raw
        GROUP BY partition_date
    )
),

orders AS (
    SELECT
        c.partition_date,
        c.product_id,
        c.variant_id,

        ao.all_orders_1d,
        ao.all_gmv_1d,
        ao.all_orders_30d,
        ao.all_gmv_30d,

        odr.orders_1d,
        odr.gmv_1d,

        SUM(odr.orders_1d) OVER (PARTITION BY c.product_id, c.variant_id ORDER BY c.partition_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS orders_30d,
        SUM(odr.gmv_1d) OVER (PARTITION BY c.product_id, c.variant_id ORDER BY c.partition_date ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS gmv_30d,

        SUM(odr.orders_1d) OVER (PARTITION BY c.product_id, c.variant_id ORDER BY c.partition_date ROWS BETWEEN 90 PRECEDING AND CURRENT ROW) > 0 AS has_sales,

        orders_30d / ao.all_orders_30d AS orders_coverage_30d,
        gmv_30d / ao.all_gmv_30d AS gmv_coverage_30d,
        odr.orders_1d / ao.all_orders_1d AS orders_coverage_1d,
        odr.gmv_1d / ao.all_gmv_1d AS gmv_coverage_1d
    FROM calendar AS c
    LEFT JOIN orders_raw AS odr USING (partition_date, product_id, variant_id)
    LEFT JOIN all_orders AS ao USING (partition_date)
),

-- превью разбила по странам, чтобы видеть, что именно мы показываем вне России
-- только 1 день, чтобы оптимизировать + preview не должны быть такими волатильными как заказы
previews_raw AS (
    SELECT
        partition_date,
        product_id,
        SUM(IF(country = 'RU', preview_count, 0)) AS previews_1d_ru,
        SUM(IF(country != 'RU', preview_count, 0)) AS previews_1d_nonru
    FROM {{ source('platform', 'context_product_counters_v5') }}
    {% if is_incremental() %}
        WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
    {% else %}
        WHERE partition_date >= DATE('2025-03-20')
    {% endif %}
    GROUP BY 1, 2
),

previews AS (
    SELECT
        partition_date,
        product_id,
        previews_1d_ru,
        previews_1d_nonru,
        previews_1d_ru / SUM(previews_1d_ru) OVER (PARTITION BY partition_date) AS previews_coverage_ru,
        previews_1d_nonru / SUM(previews_1d_nonru) OVER (PARTITION BY partition_date) AS previews_coverage_nonru,
        (previews_1d_ru + previews_1d_nonru) / SUM(previews_1d_ru + previews_1d_nonru) OVER (PARTITION BY partition_date) AS previews_coverage
    FROM previews_raw
)


SELECT
    pi1688.partition_date,

    pi1688.product_id,
    pi1688.variant_id,

    pi1688.merchant_price,
    pi1688.1688_min_price,
    pi1688.1688_median_price,
    pi1688.1688_target_price,

    pi1688.price_index,
    pi1688.product_pi_status,

    p.merchant_id,
    p.store_id,

    k.kams,

    m.merchant_name,

    p.business_line,

    mc.l1_merchant_category_name,
    mc.l2_merchant_category_name,
    mc.l3_merchant_category_name,
    mc.l4_merchant_category_name,
    mc.l5_merchant_category_name,

    o.has_sales,
    o.orders_1d,
    o.gmv_1d,
    o.orders_30d,
    o.gmv_30d,
    o.orders_coverage_1d,
    o.gmv_coverage_1d,
    o.orders_coverage_30d,
    o.gmv_coverage_30d,

    previews.previews_1d_ru / pi1688.parsed_variants_per_product AS previews_1d_ru,
    previews.previews_1d_nonru / pi1688.parsed_variants_per_product AS previews_1d_nonru,
    previews.previews_coverage_ru / pi1688.parsed_variants_per_product AS previews_coverage_ru,
    previews.previews_coverage_nonru / pi1688.parsed_variants_per_product AS previews_coverage_nonru,
    previews.previews_coverage / pi1688.parsed_variants_per_product AS previews_coverage,

    COALESCE(l.is_js, FALSE) AS is_js_approved,
    COALESCE(l.is_fbj, FALSE) AS is_fbj,
    p.created_date_utc > pi1688.partition_date - 90 AS is_new_product

FROM price_index AS pi1688

LEFT JOIN orders AS o USING (product_id, variant_id, partition_date)
LEFT JOIN previews AS previews USING (product_id, partition_date)
LEFT JOIN labels AS l USING (product_id, partition_date)

LEFT JOIN {{ ref('gold_products') }} AS p ON p.product_id = pi1688.product_id
LEFT JOIN {{ ref('gold_merchants') }} AS m ON m.merchant_id = p.merchant_id
LEFT JOIN kams AS k ON k.merchant_id = p.merchant_id AND DATE(DATE_TRUNC('QUARTER', pi1688.partition_date)) = k.partition_quarter
LEFT JOIN {{ ref('gold_merchant_categories') }} AS mc ON p.merchant_category_id = mc.merchant_category_id

