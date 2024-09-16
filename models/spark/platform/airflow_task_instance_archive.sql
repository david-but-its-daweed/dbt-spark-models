{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table'
) }}

SELECT task_id,
       dag_id,
       run_id,
       max(state) as state,
       max(priority_weight) as priority_weight,
       min(start_date) as start_date,
       min(end_date)   as end_date,
       min(duration)   as duration,
       max(try_number) as try_number
FROM platform.airflow_task_instance
where start_date > NOW() - interval 91 days
group by task_id, dag_id, run_id
