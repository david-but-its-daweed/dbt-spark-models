{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table'
) }}

WITH airflow_data AS (
    SELECT
        dag_id,
        task_id,
        partition_date,
        duration,
        start_date,
        end_date
    FROM {{ ref("airflow_task_instance_daily") }}
    WHERE partition_date > NOW() - INTERVAL 2 MONTH
),

ready_hours AS (
    SELECT
        airflow_data.dag_id,
        airflow_data.task_id,
        esd.effective_start_hours,
        (UNIX_TIMESTAMP(airflow_data.end_date) - UNIX_TIMESTAMP(DATE(airflow_data.end_date))) / 60 / 60 AS ready_time_hours
    FROM airflow_data
    LEFT JOIN {{ ref("effective_start_dates_tasks") }} AS esd
        USING (dag_id, task_id, partition_date)
)

SELECT
    dag_id,
    task_id,
    PERCENTILE_APPROX(ready_time_hours, 0.5) AS median_end_time,
    PERCENTILE_APPROX(ready_time_hours - effective_start_hours, 0.5) AS p50_effective_duration,
    PERCENTILE_APPROX(ready_time_hours - effective_start_hours, 0.8) AS p80_effective_duration
FROM ready_hours
GROUP BY dag_id, task_id