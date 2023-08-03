{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    }
) }}

SELECT *
FROM {{ref("product_outliers_seed")}}
