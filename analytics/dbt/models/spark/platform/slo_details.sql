{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    }
) }}

SELECT *
FROM {{ref("slo_details_seed")}}
