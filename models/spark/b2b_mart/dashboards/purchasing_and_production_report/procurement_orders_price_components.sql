{{ config(
    schema='b2b_mart',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH currency_rate AS (
    SELECT
        effective_date AS dt,
        currency_code,
        MAX(rate) AS rate
    FROM models.dim_pair_currency_rate
    WHERE currency_code_to = 'USD'
      AND effective_date >= '2023-01-01'
    GROUP BY effective_date, currency_code
),

initial_components AS (
    SELECT
        _id AS procurement_order_id,
        variant.value.opVId AS variant_id,
        component.key AS component,
        component.value.ccy AS currency,
        component.value.amount / 1000000 AS initial_amount,
        0 AS final_amount
    FROM mongo.b2b_core_analytic_order_product_prices_daily_snapshot
    LATERAL VIEW EXPLODE(MAP_ENTRIES(initPrices.variants)) AS variant
    LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.value.perItemComponents)) AS component   
),

final_components AS (
    SELECT
        _id AS procurement_order_id,
        variant.value.opVId AS variant_id,
        component.key AS component,
        component.value.ccy AS currency,
        0 AS initial_amount,
        component.value.amount / 1000000 AS final_amount
    FROM mongo.b2b_core_analytic_order_product_prices_daily_snapshot
    LATERAL VIEW EXPLODE(MAP_ENTRIES(finalPrices.variants)) AS variant
    LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.value.perItemComponents)) AS component   
),

components AS (
    SELECT
        procurement_order_id,
        variant_id,
        component,
        currency,
        SUM(initial_amount) AS initial_amount,
        SUM(final_amount) AS final_amount
    FROM (
        SELECT *
        FROM initial_components
        UNION ALL
        SELECT *
        FROM final_components
    ) AS m
    GROUP BY 1,2,3,4
)


SELECT
    *
FROM components
