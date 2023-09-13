{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'priority_weight': '150',
      'model_owner' : '@yushkov',
      'bigquery_load': 'true'
    }
) }}

SELECT *
FROM {{ref("product_outliers_seed")}}
