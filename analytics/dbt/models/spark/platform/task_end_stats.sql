{{ config(
    schema='platform',
    materialized='table'
) }}

with data as (select dag_id,
                     task_id,
                     to_date(CASE
                                 WHEN hour(start_date) >= 22 THEN date_trunc('Day', start_date)
                                 ELSE date_trunc('Day', start_date) - interval 24 hours
                         END) as date,
                     duration,
                     start_date,
                     end_date
              from platform.airflow_task_instance
              where start_date > NOW() - interval '2' month),

     hours as (SELECT dag_id,
                      task_id,
                      effective_start_hours,
                      (unix_timestamp(end_date) - unix_timestamp(date)) / 60 / 60 - 24 as ready_time_hours
               from data
                left join {{ref("effective_start_dates")}} using (dag_id, task_id, date)
    )

SELECT dag_id,
       task_id,
       percentile_approx(ready_time_hours, 0.5) as median_end_time,
       percentile_approx(ready_time_hours - effective_start_hours, 0.5) as p50_effective_duration,
       percentile_approx(ready_time_hours - effective_start_hours, 0.8) as p80_effective_duration
FROM hours
group by dag_id, task_id