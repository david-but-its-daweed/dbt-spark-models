{{
  config(
    materialized='table',
    meta = {
      'model_owner' : '@gusev',
      'priority_weight': '100'
      'bigquery_load': 'true'
    }
  )
}}

WITH gmv_per_day AS (
    SELECT
        partition_date AS date_msk,
        shipping_country AS country_code,
        SUM(gmv_initial - vat_initial) AS gmv_net_of_vat,
        SUM(
            -- цена мерчанта
            merchant_revenue_initial

            -- цена логистики
            + (
                COALESCE(jl_shipping_info.iteminfo.pricecomponents.itemcost.amount, 0)
                + COALESCE(jl_shipping_info.iteminfo.pricecomponents.weightcost.amount, 0)
                -- осознанно не включаем jl_shipping_info.itemInfo.priceComponents.vat.amount
                + COALESCE(jl_shipping_info.iteminfo.pricecomponents.customsduty.amount, 0)
                + COALESCE(jl_shipping_info.iteminfo.pricecomponents.refundmarkup.amount, 0)
                + COALESCE(jl_shipping_info.iteminfo.pricecomponents.insurancefee.amount, 0)
                + COALESCE(jl_shipping_info.iteminfo.pricecomponents.splitmarkup.amount, 0)
            ) / 1e6 * product_quantity
        ) AS base_price,
        SUM(gmv_initial) AS gmv,
        SUM(order_gross_profit_final_estimated) AS gross_profit
    FROM
        {{ source('mart', 'fact_order_2020') }}
    WHERE
        partition_date >= CAST('2023-01-01' AS DATE)
        AND
        -- не хотим захватить неправильную цену доставки из-за другой валюты
        (
            jl_shipping_info.iteminfo.pricecomponents.weightcost.amount IS NULL
            OR jl_shipping_info.iteminfo.pricecomponents.weightcost.ccy = 'USD'
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
        -- считаем метрики с окном в 14 дней для ислючения шума
        SUM(gmv_per_day.gmv_net_of_vat) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gmv_net_of_vat_14,
        SUM(gmv_per_day.base_price) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS base_price_14,
        SUM(gmv_per_day.gmv) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gmv_14,
        SUM(gmv_per_day.gross_profit + COALESCE(adtech.adtech_revenue, 0)) OVER (PARTITION BY 1, 2 ORDER BY country_code, date_msk ROWS BETWEEN 13 PRECEDING AND 0 FOLLOWING) AS gross_profit_14
    FROM
        gmv_per_day
    -- "LEFT JOIN" для того, чтобы не потерялись какие-то мелкие страны.
    -- При этом в кубе хранятся не только страны, они попасть не должны
    LEFT JOIN adtech USING (date_msk, country_code)
)

SELECT
    date_msk,
    country_code,
    gmv_net_of_vat_14,
    gmv_14,
    base_price_14,
    gross_profit_14,
    0.125 AS target_gmv_overhead_costs,
    0.2 AS target_margin
FROM main
ORDER BY 1
