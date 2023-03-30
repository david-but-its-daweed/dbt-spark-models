{{ config(
    schema='platform',
    materialized='view'
) }}

select output_dag_id         as dag_id,
       output_task_id        as task_id,
       date,
       coalesce(max(ready_time_hours), 0) as effective_start_hours
from platform.data_readiness
where input_rank = 2
group by output_dag_id, output_task_id, date