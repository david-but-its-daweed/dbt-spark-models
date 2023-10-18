{{
  config(
    meta = {
      'model_owner' : '@yushkov'
    },
    materialized='table',
    schema='jl_models',
  )
}}

WITH weeks AS (
    SELECT DISTINCT year_week AS partition_date
    FROM {{ source ('mart', 'dim_date') }}
    WHERE
        1 = 1
        AND id >= DATE '2023-08-28'
        AND id < CURRENT_DATE
),

cny_to_usd AS (
    SELECT
        w.partition_date,
        dcr.rate
    FROM {{ source ('mart', 'dim_pair_currency_rate') }} AS dcr
    INNER JOIN weeks AS w
        ON dcr.effective_date = w.partition_date
    WHERE
        1 = 1
        AND dcr.currency_code_to = 'USD'
        AND dcr.currency_code = 'CNY'
)

SELECT
    partition_date,
    ali.country_code AS country,
    ali.shipping_channel AS channel_id,
    CASE
        WHEN ali.danger_kind = 'General goods' THEN 'non_dangerous'
        WHEN ali.danger_kind = 'Battery included' THEN 'dangerous'
        WHEN ali.danger_kind = 'Liquid' THEN 'dangerous'
        ELSE 'non_dangerous'
    END AS dangerous_type,
    ROUND((ali.min_weight_delivery_price - ali.min_weight_g * ali.tariff_per_g) * r.rate, 2) AS per_item,
    ROUND((ali.tariff_per_g * 1e3) * r.rate, 2) AS per_kg,
    'CNY' AS ccy,
    MIN(ali.min_weight_g / 1e3) AS min_weight,
    MAX(ali.max_weight_g / 1e3) AS max_weight,
    MIN(ali.goods_price_min) AS min_price,
    MAX(ali.goods_price_max) AS max_price
FROM {{ source ('mi_analytics', 'aliexpress_logistics_prices') }} AS ali
INNER JOIN weeks
    USING (partition_date)
INNER JOIN cny_to_usd AS r
    USING (partition_date)
WHERE ali.country_code IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6, 7