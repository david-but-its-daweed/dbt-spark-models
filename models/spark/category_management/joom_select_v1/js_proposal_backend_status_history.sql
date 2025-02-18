{{
  config(
    enabled=false,
    materialized='table',
    alias='js_proposal_backend_status_history',
    file_format='parquet',
    schema='category_management',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

WITH variants AS (
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
    b.proposal_price * c.rate AS target_price_usd,
    ROUND(b.proposal_price * c.rate / b.current_price * c.rate, 3) AS discount_koef
FROM {{ ref('js_proposal_backend_status_history_raw') }} AS b
LEFT JOIN {{ ref('products_with_target_price') }} AS p
    ON
        p.proposal_id = b.proposal_id
        AND p.variant_id = b.variant_id
LEFT JOIN variants AS v
    ON
        b.product_id = v.product_id
        AND b.variant_id = v.variant_id
LEFT JOIN currency_rates AS c
    ON
        v.currency = c.currency_code
        AND b.created_date >= c.effective_date
        AND b.created_date < c.next_effective_date