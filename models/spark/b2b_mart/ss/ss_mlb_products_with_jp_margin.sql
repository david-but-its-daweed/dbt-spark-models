{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "partition_date"
    },
    meta = {
      'model_owner' : '@a.badoyan',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


SELECT partition_date,
       product_id,
       title,
       category_name,
       l1_name,
       l2_name,
       l3_name,
       l4_name,
       price_amount,
       jp_product_id,
       jp_price,
       jp_margin
FROM {{ source('joompro_analytics_mart', 'cube_mlb_products') }}
WHERE jp_margin IS NOT NULL
