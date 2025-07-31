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


WITH currency_rates_flat AS (
    SELECT
        _id,
        key AS currency_pair,
        value.exchRate / 1000000 AS exch_rate
    FROM {{ source('mongo', 'b2b_core_quotes_daily_snapshot') }}
    LATERAL VIEW explode(currencyRates) AS key, value
),

pc AS (
    SELECT
        q._id AS quote_id,
        q.dealId AS deal_id,
        q.dealFriendlyId AS deal_friendly_id,
        product.customerRequestID AS customer_request_id,
        variant.variantId AS variant_id,
        pc.key AS price_component,
        pc.value.ccy AS price_component_ccy,
        pc.value.amount / 1000000 AS price_component_amount
    FROM {{ source('mongo', 'b2b_core_quotes_daily_snapshot') }} AS q
    LATERAL VIEW EXPLODE(products) AS product
    LATERAL VIEW EXPLODE(product.variants) AS variant
    LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.priceComponents)) AS pc
)


SELECT
    p.*,
    CASE
        WHEN price_component_ccy = 'BRL' THEN 1
        ELSE cr.exch_rate
    END AS exch_rate_to_brl,
    CASE
        WHEN price_component_ccy = 'BRL'
            THEN price_component_amount
        ELSE price_component_amount * cr.exch_rate
    END AS price_component_amount_brl,
    CASE
        WHEN price_component_ccy = 'USD' THEN 1
        ELSE cr_usd.exch_rate
    END AS exch_rate_to_usd,
    CASE
        WHEN price_component_ccy = 'USD'
            THEN price_component_amount
        ELSE price_component_amount * cr_usd.exch_rate
    END AS price_component_amount_usd
FROM pc AS p
LEFT JOIN currency_rates_flat AS cr
    ON p.quote_id = cr._id
   AND CONCAT(p.price_component_ccy, '-BRL') = cr.currency_pair
LEFT JOIN currency_rates_flat AS cr_usd
    ON p.quote_id = cr_usd._id
   AND CONCAT(p.price_component_ccy, '-USD') = cr_usd.currency_pair
