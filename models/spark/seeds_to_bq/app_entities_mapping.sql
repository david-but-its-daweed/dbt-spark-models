{{
  config(
    materialized='table',
    file_format='parquet',
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true'
    }
  )
}}

SELECT *
FROM {{ ref('app_entities_mapping_seed') }}