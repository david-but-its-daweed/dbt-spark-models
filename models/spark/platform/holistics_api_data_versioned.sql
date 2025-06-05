{{ config (
    schema='holistics',
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='delta',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true'
    },
    partition_by=['date_msk'],
    ) 
}}

SELECT
    CURRENT_DATE() AS date_msk,
    *
FROM {{ ref('holistics_api_data') }}
