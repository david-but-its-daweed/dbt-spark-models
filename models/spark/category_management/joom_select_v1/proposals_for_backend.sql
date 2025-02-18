{{
  config(
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

WITH tbl AS (
    SELECT
        SHA(CONCAT(product_id, partition_date)) AS proposal_id,
        DATE('{{ var("start_date_ymd") }}') as partition_date,
        product_id,
        variant_id,
        target_price * original_currency_rate / 1e6 AS price,
        "new" AS type,
        "new" AS current_status,
        "" AS cancel_reason
    FROM {{ ref('focus_products_target_price') }} 
    WHERE partition_date = DATE('{{ var("start_date_ymd") }}')
)

SELECT
    proposal_id,
    partition_date,
    product_id,
    type,
    current_status,
    cancel_reason,
    ARRAY_AGG(STRUCT(variant_id, price)) AS target_prices
FROM tbl
GROUP BY 1, 2, 3, 4, 5, 6
