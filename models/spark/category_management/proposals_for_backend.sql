{{
  config(
    enabled=false,
    materialized='incremental',
    alias='proposals_for_backend',
    file_format='parquet',
    schema='category_management',
    incremental_strategy='insert_overwrite',
    partition_by=['partition_date'],
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'false'
    },
  )
}}
--------------------------------------------------------------------------------------
----------------- Before we send new proposals to backend ----------------------------
----------------- We must convert usd price to merchant currency price ---------------
--------------------------------------------------------------------------------------

WITH variants AS (
    SELECT
        variant_id,
        product_id,
        currency
    FROM {{ source('mart', 'dim_published_variant_with_merchant') }}
    WHERE next_effective_ts > "9999-12-31"
),

currency_rates AS (
    SELECT
        currency_code,
        rate / 1000000 AS rate,
        effective_date,
        next_effective_date
    FROM {{ source('mart', 'dim_currency_rate') }}
),

products_n_variants AS (
    SELECT
        v.product_id,
        v.variant_id,
        v.currency,
        c.rate
    FROM variants AS v
    LEFT JOIN currency_rates AS c
        ON
            c.currency_code = v.currency
            AND DATE('{{ var("start_date_ymd") }}') >= c.effective_date
            AND DATE('{{ var("start_date_ymd") }}') < c.next_effective_date
),
------- Collect information for new proposals
------- Find documentation https://www.notion.so/joomteam/Joom-Select-bfbc28a1ef53447cab283b91b9d4328a#6438a48f3f2e43e39cd14d1e02a6081d

new_proposals AS
(
    SELECT
        t.proposal_id,
        t.partition_date,
        t.product_id,
        t.variant_id,
        t.target_price / p.rate AS price,
        "new" AS type,
        "new" AS current_status,
        "" AS cancel_reason
    FROM {{ ref('products_with_target_price') }} AS t
    LEFT JOIN products_n_variants AS p
        ON
            t.product_id = p.product_id
            AND t.variant_id = p.variant_id
    WHERE t.partition_date = DATE('{{ var("start_date_ymd") }}')
),
------- Looking for products with bad rating for exelling them from JS

products_w_bad_rating AS
(
    SELECT
        product_id,
        partition_date
    FROM {{ ref('initial_metrics_set') }} AS t
    WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
        AND product_rating_60_days < 4.3
),

products_from_black_list AS (
    SELECT
        product_id
    FROM {{ source('category_management', 'joom_select_product_black_list') }}
    GROUP BY 1
),

proposals_collections AS (
------- New proposals
    SELECT
        proposal_id,
        partition_date,
        product_id,
        type,
        current_status,
        cancel_reason,
        ARRAY_AGG(STRUCT(variant_id, price)) AS  target_prices
    FROM new_proposals
    GROUP BY 1, 2, 3, 4, 5, 6
    UNION ALL
------- Proposals to cancel because of black list
    SELECT
        h.proposal_id,
        DATE('{{ var("start_date_ymd") }}') AS partition_date,
        h.product_id,
        "cancel" AS type,
        h.current_status AS current_status,
        "productRemovedFromJoomSelect" AS cancel_reason,
        NULL AS  target_prices
    FROM {{ ref('js_proposal_backend_status_history_raw') }} AS h
    INNER JOIN products_from_black_list AS bl
        ON h.product_id = bl.product_id
    WHERE status_effective_to IS NULL
        AND current_status != "cancelled"
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    UNION ALL
------- Proposals to cancel because of bad rating 
    SELECT
        h.proposal_id,
        t.partition_date,
        h.product_id,
        "cancel" AS type,
        h.current_status AS current_status,
        "productRemovedFromJoomSelect" AS cancel_reason,
        NULL AS  target_prices
    FROM {{ ref('js_proposal_backend_status_history_raw') }} AS h
    INNER JOIN products_w_bad_rating AS t
        ON h.product_id = t.product_id
    WHERE status_effective_to IS NULL
        AND current_status != "cancelled"
    GROUP BY 1, 2, 3, 4, 5, 6, 7
)

SELECT
    proposal_id,
    partition_date,
    product_id,
    type,
    current_status,
    cancel_reason,
    target_prices
FROM proposals_collections