{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    },
    tags=['data_readiness']

) }}


SELECT *,
  cast(expected_time_utc_hours_str as float) as expected_time_utc_hours
FROM {{ref("slo_details_seed")}}
