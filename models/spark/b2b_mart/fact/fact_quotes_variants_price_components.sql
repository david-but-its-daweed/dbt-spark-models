{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@abadoyan',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT product.customerRequestID AS customer_request_id,
       variant.variantId AS variant_id,
       pc.key AS price_component,
       pc.value.ccy AS price_component_ccy,
       pc.value.amount AS price_component_amount
FROM {{ source('mongo', 'b2b_core_quotes_daily_snapshot') }}
LATERAL VIEW EXPLODE(products) AS product
LATERAL VIEW EXPLODE(product.variants) AS variant
LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.priceComponents)) AS pc
