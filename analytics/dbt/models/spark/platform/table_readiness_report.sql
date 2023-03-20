{{ config(
    schema='platform',
    materialized='view'
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
    (ready_time_hours - effective_start_hours) / p50_effective_duration as slowness_ratio,
    (ready_time_hours - effective_start_hours - p50_effective_duration) * 60 as slowdown_minutes,
    state,
    input_rank,
    dag_id,
    task_id,
    priority_weight,
    start_time_human,
    round(effective_start_hours - 0.5) + round((effective_start_hours - round(effective_start_hours - 0.5))* 60)/100 as effective_start_human,
    ready_time_hours,
    (ready_time_hours - effective_start_hours) * 60 as effective_duration_minutes,
    p50_effective_duration * 60 as p50_effective_duration_minutes,
    p80_effective_duration * 60 as p80_effective_duration_minutes
from platform.data_readiness
    left join platform.task_end_stats using (dag_id, task_id)
    left join platform.effective_start_dates using (dag_id, task_id, date)