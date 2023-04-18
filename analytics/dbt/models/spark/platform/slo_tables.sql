{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    },
    tags=['data_readiness']
) }}

SELECT *
FROM {{ref("slo_tables_seed")}}
