{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true'
    }
) }}


WITH currency_rates AS (
    SELECT
        _id,
        COLLECT_LIST(
            NAMED_STRUCT(
                'currency', key,
                'exchRate', value.exchRate / 1000000
            )
        ) AS currency_rates
    FROM mongo.b2b_core_quotes_daily_snapshot
    LATERAL VIEW explode(currencyRates) AS key, value
    GROUP BY 1
)


SELECT product.customerRequestID AS customer_request_id,
       variant.variantId AS variant_id,
       pc.key AS price_component,
       pc.value.ccy AS price_component_ccy,
       pc.value.amount / 1000000 AS price_component_amount,
       cr.currency_rates
FROM {{ source('mongo', 'b2b_core_quotes_daily_snapshot') }} AS q
LEFT JOIN currency_rates AS cr ON q._id = cr._id
LATERAL VIEW EXPLODE(products) AS product
LATERAL VIEW EXPLODE(product.variants) AS variant
LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.priceComponents)) AS pc
