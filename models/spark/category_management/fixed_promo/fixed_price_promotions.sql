{{
  config(
    materialized='table',
    alias='fixed_price_promotions',
    schema='category_management',
    file_format='parquet',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true'
    }
  )
}}

WITH fpp AS (
    SELECT
        _id.g AS product_group_id,
        p AS product_id,
        _id.v AS product_variant_id,
        tP.amount / 1e6 AS promo_price,
        tP.ccy AS currency
    FROM
        {{ source('mongo','product_fixed_price_variant_items_v2_daily_snapshot') }}
),

promo AS (
    SELECT
        promo_id,
        promo_title,
        product_group_id,
        product_id,
        promo_start_time_utc,
        promo_end_time_utc
    FROM
        {{ source('mart','promotions') }}
    WHERE
        discount IS NULL
        AND TO_DATE(promo_start_time_utc) >= '2024-01-01'
)

SELECT
    mp.promo_id,
    mp.promo_title,
    TO_DATE(mp.promo_start_time_utc) AS promo_start_date,
    TO_DATE(mp.promo_end_time_utc) AS promo_end_date,
    fpp.product_group_id,
    fpp.product_id,
    fpp.product_variant_id,
    fpp.promo_price,
    fpp.currency
FROM
    fpp
INNER JOIN
    promo AS mp ON fpp.product_group_id = mp.product_group_id AND fpp.product_id = mp.product_id