{{ config(
    schema='platform',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    }
) }}

SELECT DISTINCT
    input_name,
    input_type,
    date,
    ready_time_human
FROM {{ref("data_readiness")}}
where date > NOW() - interval 2 month 
and date < NOW()
