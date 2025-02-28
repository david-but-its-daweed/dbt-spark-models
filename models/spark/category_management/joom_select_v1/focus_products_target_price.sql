{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='focus_products_target_price',
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
-- TODO: заложить логику approved > cancelled > pending
WITH manual_prices_1688 AS (
    SELECT
        product_id,
        variant_id,
        MIN(target_price) AS target_price_1688
    FROM {{ source('category_management', 'js_focus_1688_products') }}
    GROUP BY 1, 2
),

focused_products_list AS (
    SELECT
        product_id,
        COUNT_IF(pl.label = 'focused') > 0 AS is_focused,
        COUNT_IF(pl.label = 'EDLP') > 0 AS is_edlp,
        COUNT_IF(pl.label = 'joom_select') > 0 AS is_js,
        COUNT_IF(mp.product_id IS NOT NULL) > 0 AS is_1688
    FROM {{ source('goods', 'product_labels') }} AS pl
    FULL JOIN manual_prices_1688 AS mp USING (product_id)
    WHERE
        {% if is_incremental() %}
            pl.partition_date = DATE('{{ var("start_date_ymd") }}')
        {% else %}
            pl.partition_date = DATE("2025-02-17")
        {% endif %}
    GROUP BY 1
    HAVING NOT is_js AND (is_focused OR is_1688)
),

orders AS (
    SELECT
        product_id,
        o.product_variant_id AS variant_id,
        SUM(o.gmv_initial) AS sum_gmv_initial,
        COUNT(o.order_id) AS orders,
        MIN(IF(o.merchant_sale_price > 0, o.merchant_sale_price / o.product_quantity, NULL)) AS min_merchant_sale_price,
        MIN(IF(o.merchant_list_price > 0, o.merchant_list_price / o.product_quantity, NULL)) AS min_merchant_list_price
    FROM {{ ref('gold_orders') }} AS o
    INNER JOIN focused_products_list USING (product_id)
    WHERE
        o.order_date_msk >= '2025-01-01'
        AND (NOT o.is_refunded OR o.refund_reason NOT IN ('cancelled_by_merchant', 'fraud'))
    GROUP BY 1, 2
    ORDER BY min_merchant_list_price
),

calendar_dt AS (
    SELECT col AS dt
    FROM (
        SELECT EXPLODE(SEQUENCE(TO_DATE('2025-01-01'), TO_DATE(DATE('{{ var("start_date_ymd") }}')), INTERVAL 1 DAY))
    )
),

dim_variants AS (
    SELECT
        product_id,
        d.variant_id,
        d.price,
        d.currency AS currency_code,
        d.effective_ts,
        d.next_effective_ts,
        d.merchant_id
    FROM {{ source('mart', 'dim_published_variant_with_merchant') }} AS d
    INNER JOIN focused_products_list USING (product_id)
    WHERE d.next_effective_ts >= '2025-01-01'
),

-- это работает только когда даг запускается за текущую дату
currency_info AS (
    SELECT
        currency_code,
        ANY_VALUE(rate) AS rate
    FROM {{ source('mart', 'dim_currency_rate') }}
    WHERE
        next_effective_date > DATE('{{ var("start_date_ymd") }}')
    GROUP BY 1
),

vars_price AS (
    SELECT
        c.dt,
        d.merchant_id,
        d.product_id,
        d.variant_id,
        MIN(d.price / ci.rate) AS price,
        MAX_BY(d.price / ci.rate, d.next_effective_ts) AS current_price,
        ANY_VALUE(currency_code) AS original_currency,
        ANY_VALUE(ci.rate) AS original_currency_rate
    FROM calendar_dt AS c
    INNER JOIN dim_variants AS d ON c.dt BETWEEN d.effective_ts AND d.next_effective_ts
    LEFT JOIN currency_info AS ci USING (currency_code)
    GROUP BY 1, 2, 3, 4
),

promo_info AS (
    SELECT
        product_id,
        p.discount,
        DATE(p.promo_start_time_utc) AS promo_start_date,
        DATE(p.promo_end_time_utc) AS promo_end_date
    FROM {{ source('mart', 'promotions') }} AS p
    INNER JOIN focused_products_list USING (product_id)
    WHERE DATE(p.promo_end_time_utc) >= '2025-01-01'
),

promos AS (
    SELECT
        c.dt,
        pi.product_id,
        MAX(pi.discount) / 100 AS discount,
        MAX(IF(c.dt = DATE('{{ var("start_date_ymd") }}'), pi.discount, NULL)) AS current_discount
    FROM calendar_dt AS c
    INNER JOIN promo_info AS pi ON c.dt BETWEEN pi.promo_start_date AND pi.promo_end_date
    GROUP BY 1, 2
),

price_history AS (
    SELECT
        vp.merchant_id,
        vp.product_id,
        vp.variant_id,
        vp.original_currency,
        vp.original_currency_rate,
        MIN(vp.price * COALESCE(1 - p.discount, 1)) AS min_price_with_promo,
        MIN(vp.price) AS min_price,
        MAX(p.current_discount) AS current_discount,
        MIN(vp.current_price) AS current_price
    FROM vars_price AS vp
    LEFT JOIN promos AS p USING (product_id, dt)
    GROUP BY 1, 2, 3, 4, 5
),

manual_discount AS (
    SELECT
        product_id,
        MAX_BY(discount, date) AS discount
    FROM category_management.joom_select_product_white_list
    GROUP BY 1
),

manual_variant_targets AS (
    SELECT
        product_id,
        variant_id,
        MAX_BY(target_price, report_date) AS target_variant_price
    FROM category_management.joom_select_variant_target_prices
    WHERE
        product_id IS NOT NULL
        AND variant_id IS NOT NULL
        AND target_price IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    DATE('{{ var("start_date_ymd") }}') AS partition_date,
    ph.merchant_id,
    ph.product_id,
    ph.variant_id,
    CASE
        WHEN fpl.is_edlp THEN 'edlp'
        WHEN fpl.is_1688 THEN '1688'
        ELSE 'focused'
    END AS product_type, -- could be focused, edlp, 1688
    ph.original_currency,
    ph.original_currency_rate,
    o.sum_gmv_initial,
    o.orders,
    ROUND(ph.current_price, 2) AS current_price,
    ph.current_discount,
    ROUND(ph.min_price, 2) AS min_historical_price,
    ROUND(ph.min_price_with_promo, 2) AS min_price_with_promo,
    ROUND(o.min_merchant_sale_price, 2) AS min_merchant_sale_price,
    ROUND(o.min_merchant_list_price, 2) AS min_merchant_list_price,
    m.rate AS merchant_discount_rate,
    d.discount AS product_discount_rate,
    COALESCE(DOUBLE(mp.target_price_1688), mvt.target_variant_price) AS variant_target_price,
    ROUND(COALESCE(
        mp.target_price_1688, -- 1688 price
        mvt.target_variant_price,
        current_price * product_discount_rate,
        current_price * merchant_discount_rate,
        LEAST(o.min_merchant_list_price, o.min_merchant_sale_price, ph.min_price_with_promo, ph.min_price) * IF(fpl.is_edlp, 0.99, 0.97)
    ), 2) AS target_price
FROM focused_products_list AS fpl
LEFT JOIN price_history AS ph USING (product_id)
LEFT JOIN manual_variant_targets AS mvt USING (product_id, variant_id)
LEFT JOIN manual_prices_1688 AS mp USING (product_id, variant_id)
LEFT JOIN manual_discount AS d USING (product_id)
LEFT JOIN category_management.js_merchant_discount_rate AS m USING (merchant_id)
LEFT JOIN orders AS o USING (product_id, variant_id)
