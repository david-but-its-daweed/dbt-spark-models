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


SELECT variant.variantId AS variant_id,
       pc.key AS price_component,
       pc.value.ccy AS price_component_ccy,
       CAST(pc.value.amount AS INT) AS price_component_amount
FROM {{ ref('scd2_mongo_quotes') }}
LATERAL VIEW EXPLODE(products) AS product
LATERAL VIEW EXPLODE(product.variants) AS variant
LATERAL VIEW EXPLODE(MAP_ENTRIES(variant.priceComponents)) AS pc
