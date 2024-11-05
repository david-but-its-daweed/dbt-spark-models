{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='incremental',
    partition_by=['partition_date'],
    incremental_strategy='insert_overwrite',
    file_format='parquet'
) }}

SELECT
    task_id,
    dag_id,
    run_id,
    map_index,
    operator,
    pool,
    start_date,
    end_date,
    duration,
    state,
    priority_weight,
    try_number,
    max_tries,
    TO_DATE(CASE
        WHEN HOUR(start_date) >= 22 THEN DATE_TRUNC('Day', start_date + INTERVAL 24 HOURS)
        ELSE DATE_TRUNC('Day', start_date)
    END) AS partition_date
FROM platform.airflow_task_instance
WHERE
    start_date IS NOT NULL
    {% if is_incremental() %}
        AND DATE(start_date) >= DATE '{{ var("start_date_ymd") }}'
        AND DATE(start_date) < DATE '{{ var("end_date_ymd") }}'
    {% else %}
  AND date(start_date) > NOW() - INTERVAL 6 MONTH
{% endif %}
