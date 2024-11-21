{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

SELECT
    partition_date,
    dag_id,
    task_id,
    MAX(effective_start_hours_msk) AS effective_start_hours_msk,
    MAX(effective_start_hours) AS effective_start_hours,
    MAX(effective_start_hours_cleared_msk) AS effective_start_hours_cleared_msk,
    MAX(effective_start_hours_cleared) AS effective_start_hours_cleared
FROM {{ ref("effective_start_dates") }}
GROUP BY
    partition_date,
    dag_id,
    task_id