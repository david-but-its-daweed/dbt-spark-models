{{
  config(
    materialized='table',
    meta = {
      'model_owner' : '@general_analytics',
      'priority_weight': '1000',
      'bigquery_load': 'true'
    }
  )
}}

WITH gmv_per_day AS (
    SELECT
        fo.partition_date AS date_msk,
        fo.shipping_country AS country_code,
        SUM(fo.gmv_initial - fo.vat_initial) AS gmv_net_of_vat,
        SUM(
            -- цена мерчанта
            fo.merchant_revenue_initial
            -- цена логистики
            + (
                COALESCE(lfo.jl_shipping_item_price_components.item_cost.amount, 0)
                + COALESCE(lfo.jl_shipping_item_price_components.weight_cost.amount, 0)
                -- осознанно не включаем jl_shipping_item_price_components.vat.amount
                + COALESCE(lfo.jl_shipping_item_price_components.customs_duty.amount, 0)
                + COALESCE(lfo.jl_shipping_item_price_components.refund_markup.amount, 0)
                + COALESCE(lfo.jl_shipping_item_price_components.insurance_fee.amount, 0)
            ) * lfo.quantity
        ) AS base_price,
        SUM(fo.gmv_initial) AS gmv,
        SUM(fo.order_gross_profit_final_estimated) AS gross_profit
    FROM
        {{ source('mart', 'fact_order_2020') }} AS fo
    LEFT JOIN {{ source('logistics_mart', 'fact_order') }} AS lfo ON lfo.order_id = fo.order_id
    WHERE
        fo.partition_date >= CAST('2023-01-01' AS DATE)
        AND lfo.order_created_date_msk >= CAST('2022-12-31' AS DATE)
        AND
        -- не хотим захватить неправильную цену доставки из-за другой валюты
        (
            lfo.jl_shipping_item_price_components.weight_cost.amount IS NULL
            OR lfo.jl_shipping_item_price_components.weight_cost.ccy = 'USD'
            AND lfo.origin_country = 'CN'
        )
    GROUP BY 1, 2
),

adtech AS (
    SELECT
        t AS date_msk,
        country AS country_code,
        SUM(adtech_revenue) AS adtech_revenue
    FROM
        {{ source('cube', 'time_orders_additional_cube_2_days') }}
    WHERE
        t >= CAST('2023-01-01' AS DATE)
        AND country != 'all'
    GROUP BY 1, 2
),

main AS (
    SELECT
        date_msk,
        country_code,
        -- считаем метрики по дням
        SUM(gmv_per_day.gmv_net_of_vat) AS gmv_net_of_vat_per_day,
        SUM(gmv_per_day.base_price) AS base_price_per_day,
        SUM(gmv_per_day.gmv) AS gmv_per_day,
        SUM(gmv_per_day.gross_profit + COALESCE(adtech.adtech_revenue, 0)) AS gross_profit_per_day,
        -- считаем метрики с окном в 14 дней для ислючения шума
        SUM(SUM(gmv_per_day.gmv_net_of_vat)) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gmv_net_of_vat_14,
        SUM(SUM(gmv_per_day.base_price)) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS base_price_14,
        SUM(SUM(gmv_per_day.gmv)) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gmv_14,
        SUM(SUM(gmv_per_day.gross_profit + COALESCE(adtech.adtech_revenue, 0))) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gross_profit_14
    FROM
        gmv_per_day
    -- "LEFT JOIN" для того, чтобы не потерялись какие-то мелкие страны.
    -- При этом в кубе хранятся не только страны, другие значение попасть не должны
    LEFT JOIN adtech USING (date_msk, country_code)
    GROUP BY 1, 2
)

SELECT
    date_msk,
    country_code,
    gmv_net_of_vat_per_day,
    gmv_per_day,
    base_price_per_day,
    gross_profit_per_day,
    gmv_net_of_vat_14,
    gmv_14,
    base_price_14,
    gross_profit_14,
    CASE
        WHEN DATE_TRUNC('QUARTER', date_msk) >= '2024-07-01' THEN 0.08
        ELSE 0.125
    END AS target_gmv_overhead_costs,
    CASE
        WHEN DATE_TRUNC('QUARTER', date_msk) >= '2024-07-01' THEN 0.216
        ELSE 0.2
    END AS target_margin
FROM main
ORDER BY 1, 2
