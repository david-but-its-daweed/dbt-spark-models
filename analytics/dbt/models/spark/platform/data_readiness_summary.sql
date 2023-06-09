{{ config(
    schema='platform_slo',
    materialized='table',
    meta = {
      'bigquery_load': 'true'
    },
    tags=['data_readiness']
) }}

SELECT DISTINCT
    source_id,
    input_name,
    input_type,
    input_rank,
    date,
    ready_time_human,
    (ready_time_hours - effective_start_hours ) * 60 effective_duration_minutes
FROM {{ref("data_readiness")}}
  left join {{ref("effective_start_dates")}} using (dag_id, task_id, date)
where date > NOW() - interval 2 month 
and date < NOW()
