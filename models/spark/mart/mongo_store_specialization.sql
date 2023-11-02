{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@gburg',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true'
    }
) }}

select _id as specialization_id,
       name,
       categories
FROM {{ source('mongo', 'core_store_specializations_daily_snapshot') }}
