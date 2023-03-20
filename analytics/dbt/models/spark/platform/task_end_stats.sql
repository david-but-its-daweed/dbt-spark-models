{{ config(
    schema='platform',
    materialized='table'
) }}

with data as (select dag_id,
                     task_id,
                     to_date(CASE
                                 WHEN hour(start_date) >= 22 THEN date_trunc('Day', start_date)
                                 ELSE date_trunc('Day', start_date) - interval 24 hours
                         END) as partition_date,
                     end_date
              from platform.airflow_task_instance
              where start_date > NOW() - interval '2' month),

     hours as (SELECT dag_id,
                      task_id,
                      (unix_timestamp(end_date) - unix_timestamp(partition_date)) / 60 / 60 - 24 as ready_time_hours
               from data)

SELECT dag_id,
       task_id,
       percentile_approx(ready_time_hours, 0.5) as median_end_time
FROM hours
group by dag_id, task_id