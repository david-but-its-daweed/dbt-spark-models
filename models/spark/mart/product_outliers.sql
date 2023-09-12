{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@yushkov',
      'bigquery_load': 'true'
    }
) }}

SELECT *
FROM {{ref("product_outliers_seed")}}
