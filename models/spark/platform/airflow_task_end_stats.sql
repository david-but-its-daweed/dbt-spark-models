{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    file_format='delta'
) }}

WITH airflow_data AS (
    SELECT
        dag_id,
        task_id,
        partition_date,
        start_date,
        end_date,
        ROUND(duration / 60) AS duration
    FROM {{ ref("airflow_task_instance_daily") }}
    WHERE
        partition_date > NOW() - INTERVAL 2 MONTH
        AND run_number = 1
),

ready_hours AS (
    SELECT
        airflow_data.dag_id,
        airflow_data.task_id,
        airflow_data.duration,

        esd.effective_start_hours_msk,
        esd.effective_start_hours_cleared_msk,

        (UNIX_TIMESTAMP(airflow_data.start_date) - UNIX_TIMESTAMP(DATE(airflow_data.start_date))) / 60 / 60 AS start_time_hours,
        ((UNIX_TIMESTAMP(airflow_data.start_date) - UNIX_TIMESTAMP(DATE(airflow_data.start_date))) / 60 / 60 + 3) % 24 AS start_time_hours_msk,

        (UNIX_TIMESTAMP(airflow_data.end_date) - UNIX_TIMESTAMP(DATE(airflow_data.end_date))) / 60 / 60 AS ready_time_hours,
        ((UNIX_TIMESTAMP(airflow_data.end_date) - UNIX_TIMESTAMP(DATE(airflow_data.end_date))) / 60 / 60 + 3) % 24 AS ready_time_hours_msk
    FROM airflow_data
    LEFT JOIN {{ ref("effective_start_dates_tasks") }} AS esd
        USING (dag_id, task_id, partition_date)
)

SELECT
    dag_id,
    task_id,
    PERCENTILE_APPROX(duration, 0.5) AS median_duration,
    PERCENTILE_APPROX(start_time_hours, 0.5) AS median_start_time,
    PERCENTILE_APPROX(ready_time_hours, 0.5) AS median_end_time,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_msk, 0.5) AS p50_effective_duration,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_msk, 0.8) AS p80_effective_duration,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_cleared_msk, 0.5) AS p50_effective_duration_cleared,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_cleared_msk, 0.8) AS p80_effective_duration_cleared
FROM ready_hours
GROUP BY dag_id, task_id