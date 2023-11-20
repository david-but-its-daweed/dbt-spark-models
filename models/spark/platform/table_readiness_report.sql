{{ config(
    schema='platform',
    materialized='view',
    tags=['data_readiness'],
    meta = {
      'model_owner' : '@gusev',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_upload_horizon_days': '2',
      'bigquery_fail_on_missing_partitions': 'false',
      'bigquery_check_counts': 'false',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks'
    },
) }}

select source_id,
    date as partition_date,
    input_name,
    input_type,
    case 
        when state = 'failed' then 999
        else coalesce(ready_time_human, 999)
    end as ready_time,
    round(median_end_time - 0.5) + round((median_end_time - round(median_end_time - 0.5))* 60)/100 as median_ready_time,
    (ready_time_hours - coalesce(effective_start_hours, 0)) / coalesce(p50_effective_duration, median_end_time) as slowness_ratio,
    (ready_time_hours - coalesce(effective_start_hours, 0) - coalesce(p50_effective_duration, median_end_time)) * 60 as slowdown_minutes,
    state,
    input_rank,
    run_id,
    dag_id,
    task_id,
    priority_weight,
    start_time_human,
    round(coalesce(effective_start_hours, 0) - 0.5) + round((coalesce(effective_start_hours, 0) - round(coalesce(effective_start_hours, 0) - 0.5))* 60)/100 as effective_start_human,
    ready_time_hours,
    (ready_time_hours - coalesce(effective_start_hours, 0)) * 60 as effective_duration_minutes,
    coalesce(p50_effective_duration, median_end_time) * 60 as p50_effective_duration_minutes,
    p80_effective_duration * 60 as p80_effective_duration_minutes
from {{ref("data_readiness")}}
    left join {{ref("task_end_stats")}} using (dag_id, task_id)
    left join {{ref("effective_start_dates")}} using (dag_id, task_id, date)
where date >= NOW() - INTERVAL 2 MONTH
