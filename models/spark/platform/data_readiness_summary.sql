{{ config(
    schema='platform_slo',
    materialized='table',
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks',
    },
    tags=['data_readiness']
) }}

SELECT DISTINCT
    dr.source_id,
    dr.input_name,
    dr.input_type,
    dr.input_rank,
    dr.dag_id,
    dr.task_id,
    dr.partition_date,
    dr.ready_time_hours,
    dr.ready_time_hours_msk,
    dr.ready_time_human,
    dr.ready_time_human_msk,
    (dr.ready_time_hours_msk - airflow_task_esd.effective_start_hours_msk) * 60 AS airflow_effective_duration_minutes,
    (dr.ready_time_hours_msk - tables_esd.effective_start_hours_msk) * 60 AS tables_effective_duration_minutes,
    COALESCE(
        (dr.ready_time_hours_msk - airflow_task_esd.effective_start_hours_msk) * 60,
        (dr.ready_time_hours_msk - tables_esd.effective_start_hours_msk) * 60
    ) AS effective_duration_minutes

FROM {{ ref("data_readiness") }} AS dr
LEFT JOIN {{ ref("effective_start_dates_tasks") }} AS airflow_task_esd
    ON
        dr.dag_id = airflow_task_esd.dag_id
        AND dr.task_id = airflow_task_esd.task_id
        AND dr.partition_date = airflow_task_esd.partition_date

LEFT JOIN {{ ref("effective_start_dates") }} AS tables_esd
    ON
        dr.input_name = tables_esd.table_name
        AND dr.input_type = tables_esd.table_type
        AND dr.partition_date = tables_esd.partition_date
WHERE
    dr.partition_date > DATE(NOW()) - INTERVAL 2 MONTH
    AND dr.partition_date <= DATE(NOW())
