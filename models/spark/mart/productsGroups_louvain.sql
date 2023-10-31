{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@gburg',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true',
      'priority_weight': '150'
    }
) }}

select *
FROM {{ source('default', 'productsGroups_louvain') }}
