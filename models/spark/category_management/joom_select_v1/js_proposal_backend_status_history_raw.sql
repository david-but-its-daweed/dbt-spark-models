{{
  config(
    materialized='view',
    alias='js_proposal_backend_status_history_raw',
    schema='category_management',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

---------------------------------------------------------------------------
--------- Here the only source is historical data about proposals ---------
---- So, this code just unpacking it from structures into flat a table ----
---------------------------------------------------------------------------
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
)

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
