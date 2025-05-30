{{
  config(
    materialized='incremental',
    on_schema_change='sync_all_columns',
    alias='js2_proposals',
    file_format='delta',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    meta = {
        'model_owner' : '@vasiukova_mn',
        'bigquery_load': 'true'
    },
  )
}}


WITH proposals_raw AS (
    SELECT
        *,
        FILTER(status_history, x -> x.status = 'approved')[0]['updated_time'] AS approved_at,
        FILTER(status_history, x -> x.status = 'cancelled')[0]['updated_time'] AS cancelled_at,
        CASE
            WHEN cancelled_at IS NOT NULL THEN 'cancelled'
            WHEN approved_at IS NOT NULL THEN 'approved'
            ELSE 'pending'
        END AS current_status,
        LAG(status_history) OVER (PARTITION BY product_id ORDER BY created_time) AS previous_sh,
        LEAD(created_time) OVER (PARTITION BY product_id ORDER BY created_time) AS next_proposal_dt
    FROM {{ source('mart', 'dim_joom_select_proposal') }}
    WHERE next_effective_ts IS NULL
),

proposals_v1 AS (
    SELECT
        proposal_id,
        created_time,
        updated_time,
        product_id,
        next_proposal_dt,
        approved_at,
        cancelled_at,
        current_status,
        EXPLODE(target_variant_prices) AS tvp,
        FILTER(status_history, x -> x.status = 'pending')[0]['merchant_variant_prices'] AS mp,
        CASE
            WHEN previous_sh IS NULL THEN NULL
            WHEN FILTER(previous_sh, x -> x.status = 'cancelled')[0]['updated_time'] IS NOT NULL THEN 'cancelled'
            WHEN FILTER(previous_sh, x -> x.status = 'approved')[0]['updated_time'] IS NOT NULL THEN 'approved'
            ELSE 'pending'
        END AS previous_proposal_status,

        cancel_info.reason AS cancel_reason,
        cancel_info.source AS cancel_source
    FROM proposals_raw
),

proposals AS (
    -- добавить price_source
    SELECT
        proposal_id,
        created_time,
        updated_time,
        product_id,
        approved_at,
        cancelled_at,
        previous_proposal_status,
        tvp.variant_id,
        tvp.price / 1e6 AS target_price,
        FILTER(mp, x -> x.variant_id = tvp.variant_id)[0]['price'] / 1e6 AS merchant_price,
        next_proposal_dt,
        COALESCE(current_status = 'approved' AND DATE_DIFF(DATE('{{ var("start_date_ymd") }}'), next_proposal_dt) <= 14, FALSE) AS is_expiring
    FROM proposals_v1
),

orders_raw AS (
    SELECT
        product_id,
        product_variant_id,
        SUM(gmv_initial) AS gmv,
        COUNT(order_id) AS orders
    FROM {{ ref('gold_orders') }}
    WHERE
        order_date_msk > DATE('{{ var("start_date_ymd") }}') - 30
        AND (NOT is_refunded OR refund_reason NOT IN ('cancelled_by_merchant', 'fraud'))
    GROUP BY 1, 2
),

orders AS (
    SELECT
        product_id,
        product_variant_id AS variant_id,
        gmv AS gmv_l30d,
        orders AS orders_l30d,
        gmv / SUM(gmv) OVER () AS gmv_share_l30d,
        orders / SUM(orders) OVER () AS orders_share_l30d
    FROM orders_raw
),

labels AS (
    SELECT
        product_id,
        COUNT_IF(label = 'EDLP') > 0 AS is_edlp,
        COUNT_IF(label = 'fbj_more_1d_stock') > 0 AS is_fbj
    FROM {{ source('goods', 'product_labels') }}
    WHERE partition_date = DATE('{{ var("start_date_ymd") }}') - 1
    GROUP BY 1
),

kams AS (
    SELECT
        merchant_id,
        REPLACE(MAX_BY(kam_email, quarter), '@joom.com', '') AS kam
    FROM {{ source('category_management', 'merchant_kam_materialized') }}
    WHERE merchant_id IS NOT NULL
    GROUP BY 1
),

manual_prices_raw AS (
    SELECT
        wl.product_id,
        wl.author,
        wl.date AS partition_date
    FROM {{ source('category_management', 'joom_select_product_white_list') }} AS wl

    UNION ALL

    SELECT DISTINCT
        tp.product_id,
        tp.author,
        tp.report_date AS partition_date
    FROM {{ source('category_management', 'joom_select_variant_target_prices') }} AS tp
),

manual_prices AS (
    SELECT
        mpr.product_id,
        MAX_BY(mpr.author, mpr.partition_date) AS manual_price_author
    FROM manual_prices_raw AS mpr
    LEFT ANTI JOIN {{ source('category_management', 'joom_select_product_black_list') }} USING (product_id)
    GROUP BY 1
)

SELECT
    p.proposal_id,
    p.product_id,
    p.variant_id,
    p.target_price,
    p.merchant_price,
    p.created_time,
    p.updated_time,
    p.approved_at,
    p.cancelled_at,
    p.previous_proposal_status,
    p.next_proposal_dt,
    p.is_expiring,

    o.gmv_l30d,
    o.orders_l30d,
    o.gmv_share_l30d,
    o.orders_share_l30d,

    ppc.merchant_id,
    ppc.product_name,
    ppc.category_id,
    ppc.store_id,
    ppc.is_public,
    ppc.created_date AS product_create_date,
    ppc.rating,
    COALESCE(ARRAY_CONTAINS(ppc.labels.key, '1688_auto_price'), FALSE) AS is_1688,

    COALESCE(l.is_edlp, FALSE) AS is_edlp,
    COALESCE(l.is_fbj, FALSE) AS is_fbj,

    m.merchant_name,
    m.origin_name,

    k.kam,

    mc.l1_merchant_category_name,
    mc.l2_merchant_category_name,
    mc.l3_merchant_category_name,
    mp.manual_price_author

FROM proposals AS p
LEFT JOIN orders AS o USING (product_id, variant_id)
LEFT JOIN {{ source('mart', 'published_products_current') }} AS ppc USING (product_id)
LEFT JOIN labels AS l USING (product_id)
LEFT JOIN {{ ref('gold_merchants') }} AS m USING (merchant_id)
LEFT JOIN kams AS k ON k.merchant_id = ppc.merchant_id
LEFT JOIN {{ ref('gold_merchant_categories') }} AS mc ON mc.merchant_category_id = ppc.category_id
LEFT JOIN manual_prices AS mp USING (product_id)