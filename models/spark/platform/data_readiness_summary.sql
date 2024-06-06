{{ config(
    schema='platform_slo',
    materialized='table',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks',
    },
    tags=['data_readiness']
) }}

SELECT DISTINCT
    source_id,
    input_name,
    input_type,
    input_rank,
    dag_id,
    task_id,
    date,
    ready_time_human,
    (ready_time_hours - effective_start_hours ) * 60 effective_duration_minutes
FROM {{ref("data_readiness")}}
  left join {{ref("effective_start_dates")}} using (dag_id, task_id, date)
where date > NOW() - interval 2 month 
and date < NOW()
