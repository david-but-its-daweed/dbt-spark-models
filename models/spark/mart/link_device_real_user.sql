{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@gburg',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true'
    }
) }}

select *
FROM {{ source('default', 'link_device_real_user') }}
