{{
  config(
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    schema='category_management',
    meta = {
        'model_owner' : '@catman-analytics.duty',
        'bigquery_load': 'true'
    }
  )
}}

SELECT *
FROM {{ source( 'category_management', 'assortment_coverage_metrics') }}