{{
  config(
    materialized='table',
    alias='js_proposal_backend_status_history',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'created_date',
        'bigquery_fail_on_missing_partitions': 'false'
    }
  )
}}

WITH backend_tab_stg_1 AS (
    SELECT
        SUBSTRING_INDEX(SUBSTRING_INDEX(proposal_id, '"', 2), '"', -1) AS proposal_id,
        created_time,
        updated_time,
        product_id,
        merchant_id,
        effective_ts,
        next_effective_ts,
        status_history,
        EXPLODE(target_variant_prices) AS col,
        cancel_info.reason,
        cancel_info.source
    FROM {{ source('mart', 'dim_joom_select_proposal') }}
),

backend_tab_stg_2 AS (
    SELECT
        proposal_id,
        created_time,
        updated_time,
        product_id,
        merchant_id,
        effective_ts,
        next_effective_ts,
        col.variant_id,
        col.price,
        reason,
        source,
        EXPLODE(status_history) AS col
    FROM backend_tab_stg_1
),

backend_tab_stg_3 AS (
    SELECT
        proposal_id,
        created_time,
        product_id,
        merchant_id,
        effective_ts,
        next_effective_ts,
        variant_id,
        price,
        reason,
        source,
        col.status,
        col.updated_time,
        EXPLODE(col.merchant_variant_prices) AS col
    FROM backend_tab_stg_2
),

backend_tab_final AS (
    SELECT
        proposal_id,
        DATE(created_time) AS created_date,
        created_time AS created_ts,
        product_id,
        merchant_id,
        variant_id,
        price / 1000000 AS proposal_price,
        reason,
        source,
        status,
        updated_time AS status_effective_from,
        LEAD(updated_time) OVER (PARTITION BY proposal_id, product_id, variant_id ORDER BY updated_time) AS status_effective_to,
        col.price / 1000000 AS current_price,
        FIRST(status) OVER (PARTITION BY proposal_id, product_id, variant_id ORDER BY updated_time DESC) AS current_status
    FROM backend_tab_stg_3
    WHERE
        col.variant_id = variant_id
        AND next_effective_ts IS NULL
),

variants AS (
    SELECT
        variant_id,
        product_id,
        price / 1000000 AS price,
        currency
    FROM {{ source('mart', 'dim_published_variant_with_merchant') }}
    WHERE next_effective_ts > '9999-12-31'
),

currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_currency_rate') }}
)

SELECT
    b.proposal_id,
    b.created_date,
    b.created_ts,
    b.product_id,
    b.merchant_id,
    b.variant_id,
    b.reason,
    b.source,
    b.status,
    b.status_effective_from,
    b.status_effective_to,
    b.current_price * c.rate AS current_price_usd,
    b.current_status,
    p.main_category,
    p.business_line,
    p.merchant_name,
    p.reason_of_participation,
    p.product_rating_60_days,
    p.gmv_60_days,
    p.orders_60_days,
    p.orders_with_nf_share_1_year,
    p.merchant_cancel_rate_1_year,
    p.merchant_price_index,
    p.current_price_usd AS initial_price_usd,
    p.target_price_reason,
    p.target_price AS target_price_usd,
    ROUND(p.target_price / p.current_price_usd, 3) AS discount_koef
FROM backend_tab_final AS b
LEFT JOIN {{ ref('products_with_target_price') }} AS p
    ON
        p.proposal_id = b.proposal_id
        AND p.variant_id = b.variant_id
LEFT JOIN variants AS v
    ON
        p.product_id = v.product_id
        AND p.variant_id = v.variant_id
LEFT JOIN currency_rates AS c
    ON
        v.currency = c.currency_code
        AND p.partition_date >= c.effective_date
        AND p.partition_date < c.next_effective_date