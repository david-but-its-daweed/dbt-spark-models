{{ config (
    schema='holistics',
    materialized='incremental',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true'
    },
    incremental_strategy='insert_overwrite',
    partition_by=['date_msk'],
    ) 
}}

SELECT
    CURRENT_DATE() AS date_msk,
    *
FROM {{ ref('holistics_api_data') }}
