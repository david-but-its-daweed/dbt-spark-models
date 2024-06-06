{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@analytics.duty',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true'
    }
) }}

select *
FROM {{ source('default', 'link_device_user') }}
