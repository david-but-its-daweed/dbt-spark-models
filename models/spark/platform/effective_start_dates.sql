{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

WITH ready_time_outputs AS (
    SELECT
        partition_date,
        input_name,
        input_type,
        MIN(ready_time_hours_msk) AS ready_time_hours_msk,
        MIN(start_time_hours_msk) AS start_time_hours_msk
    FROM {{ ref("data_readiness") }}
    WHERE input_rank = 2
    GROUP BY
        partition_date,
        input_name,
        input_type
)

SELECT
    dr.partition_date,
    dr.output_name AS table_name,
    dr.output_type AS table_type,
    dr.output_dag_id AS dag_id,
    dr.output_task_id AS task_id,

    COALESCE(MAX(dr.ready_time_hours_msk), 0) AS effective_start_hours_msk,
    COALESCE(MAX(dr.ready_time_hours), 0) AS effective_start_hours,

    COALESCE(MAX(CASE WHEN rto.start_time_hours_msk >= dr.ready_time_hours_msk THEN dr.ready_time_hours_msk END), 0) AS effective_start_hours_cleared_msk,
    COALESCE(MAX(CASE WHEN rto.start_time_hours_msk >= dr.ready_time_hours_msk THEN dr.ready_time_hours END), 0) AS effective_start_hours_cleared

FROM {{ ref("data_readiness") }} AS dr
LEFT JOIN ready_time_outputs AS rto
    ON
        dr.output_name = rto.input_name
        AND dr.output_type = rto.input_type
        AND dr.partition_date = rto.partition_date

WHERE dr.input_rank = 2
GROUP BY
    dr.partition_date,
    dr.output_name,
    dr.output_type,
    dr.output_dag_id,
    dr.output_task_id