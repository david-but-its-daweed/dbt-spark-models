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


SELECT _id AS quote_id,
       dealId AS deal_id,
       product.productId AS product_id,
       --product.productName AS product_name,
       --product.totalQuantity AS product_total_quantity,
       --product.totalPrice.amount AS product_total_price,
       --product.totalPrice.ccy AS product_total_price_ccy,
       variant.variantId AS variant_id,
       variant.quantity,
       variant.ddpPerItem.amount AS ddp_per_item,
       variant.ddpPerItem.ccy AS ddp_per_item_ccy,
       variant.exwTotalPrice.amount AS exw_total_price,
       variant.exwTotalPrice.ccy AS exw_total_price_ccy,
       variant.totalPrice.amount AS total_price,
       variant.totalPrice.ccy AS total_price_ccy
FROM {{ ref('scd2_mongo_quotes') }}
LATERAL VIEW EXPLODE(products) AS product
LATERAL VIEW EXPLODE(product.variants) AS variant
