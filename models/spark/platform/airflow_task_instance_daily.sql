{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}
WITH airflow_data AS (
    SELECT
        task_id,
        dag_id,
        run_id,
        operator,
        partition_date,
        state,
        priority_weight,
        start_date,
        end_date,
        duration,
        try_number,
        pool
    FROM {{ ref("airflow_task_instance_archive") }}
    WHERE
        partition_date < TO_DATE(NOW()) - INTERVAL 1 DAY
        AND partition_date >= TO_DATE(NOW()) - INTERVAL 3 MONTH
        AND state <> 'skipped'

    UNION ALL

    SELECT
        task_id,
        dag_id,
        run_id,
        operator,
        TO_DATE(CASE
            WHEN HOUR(start_date) >= 22 THEN DATE_TRUNC('Day', start_date) + INTERVAL 24 HOURS
            ELSE DATE_TRUNC('Day', start_date)
        END) AS partition_date,
        state,
        priority_weight,
        start_date,
        end_date,
        duration,
        try_number,
        pool
    FROM platform.airflow_task_instance
    WHERE
        start_date >= DATE_SUB(TO_DATE(NOW()), 1) - INTERVAL 2 HOUR
        AND state <> 'skipped'
)

SELECT
    task_id,
    dag_id,
    run_id,
    operator,
    partition_date,
    state,
    priority_weight,
    start_date,
    end_date,
    duration,
    try_number,
    pool,
    ROW_NUMBER() OVER (PARTITION BY task_id, dag_id, partition_date ORDER BY start_date ASC) AS run_number,
    COUNT(*) OVER (PARTITION BY task_id, dag_id, partition_date) AS run_cnt
FROM airflow_data