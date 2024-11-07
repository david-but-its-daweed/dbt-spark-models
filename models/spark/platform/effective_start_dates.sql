{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

SELECT
    partition_date,
    output_name AS table_name,
    output_type AS table_type,
    output_dag_id AS dag_id,
    output_task_id AS task_id,
    COALESCE(MAX(ready_time_hours_msk), 0) AS effective_start_hours_msk,
    COALESCE(MAX(ready_time_hours), 0) AS effective_start_hours
FROM {{ ref("data_readiness") }}
WHERE input_rank = 2
GROUP BY
    partition_date,
    output_name,
    output_type,
    output_dag_id,
    output_task_id