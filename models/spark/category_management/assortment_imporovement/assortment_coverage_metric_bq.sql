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

SELECT
    *,
    MAX(partition_date) OVER () AS last_partition_date,
    DATE_TRUNC("week", partition_date) AS partition_week
FROM {{ source( 'category_management', 'assortment_coverage_metrics') }}