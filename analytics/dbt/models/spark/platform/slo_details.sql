{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'bigquery_load': 'true'
    }
) }}

SELECT *,
  cast(expected_time_utc_hours_str as int) as expected_time_utc_hours
FROM {{ref("slo_details_seed")}}
