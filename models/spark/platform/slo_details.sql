{{ config(
    schema='platform_slo',
    materialized='view',
    meta = {
      'model_owner' : '@vladimir',
      'bigquery_load': 'true',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks'
    },
    tags=['data_readiness']

) }}


SELECT *,
  cast(expected_time_utc_hours_str as float) as expected_time_utc_hours
FROM {{ref("slo_details_seed")}}
