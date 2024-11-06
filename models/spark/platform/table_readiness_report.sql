{{ config(
    schema='platform',
    materialized='view',
    tags=['data_readiness'],
    meta = {
      'model_owner' : '@general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date',
      'bigquery_upload_horizon_days': '2',
      'bigquery_fail_on_missing_partitions': 'false',
      'bigquery_check_counts': 'false',
      'airflow_pool': 'k8s_platform_hourly_spark_tasks'
    },
) }}

WITH final_data AS (
    SELECT
        dr.source_id,
        dr.partition_date,
        dr.run_id,
        dr.input_name,
        dr.input_type,
        dr.input_rank,
        dr.dag_id,
        dr.task_id,
        dr.output_name,
        dr.output_type,
        dr.output_dag_id,
        dr.output_task_id,
        dr.ready_time_hours,
        dr.ready_time_human,
        dr.start_time_hours,
        dr.start_time_human,
        dr.state,
        dr.priority_weight,
        dr.ftu_start_date,
        dr.ftu_end_date,
        dr.airflow_start_date,
        dr.airflow_end_date,
        dr.start_date,
        dr.end_date,
        dr.airflow_duration,
        dr.ftu_duration,
        dr.duration,
        dr.try_number,
        dr.is_daily,
        dr.is_output_has_ftu_sensor,
        dr.is_manual_table,

        at_es.median_end_time AS airflow_median_ready_time,
        at_es.p50_effective_duration AS airflow_p50_effective_duration,
        at_es.p80_effective_duration AS airflow_p80_effective_duration,

        ftu_es.median_end_time AS ftu_median_ready_time,
        ftu_es.p50_effective_duration AS ftu_p50_effective_duration,
        ftu_es.p80_effective_duration AS ftu_p80_effective_duration,

        COALESCE(ftu_es.median_end_time, at_es.median_end_time) AS median_ready_time,
        COALESCE(ftu_es.p50_effective_duration, at_es.p50_effective_duration) AS p50_effective_duration,
        COALESCE(ftu_es.p80_effective_duration, at_es.p80_effective_duration) AS p80_effective_duration,


        airflow_task_esd.effective_start_hours AS airflow_task_effective_start_hours,
        tables_esd.effective_start_hours AS tables_effective_start_hours,
        COALESCE(tables_esd.effective_start_hours, airflow_task_esd.effective_start_hours) AS effective_start_hours,

        dr.ready_time_hours - COALESCE(tables_esd.effective_start_hours, airflow_task_esd.effective_start_hours, 0) AS effective_duration

    FROM {{ ref("data_readiness") }} AS dr

    LEFT JOIN {{ ref("airflow_task_end_stats") }} AS at_es
        ON
            dr.dag_id = at_es.dag_id
            AND dr.task_id = at_es.task_id

    LEFT JOIN {{ ref("ftu_end_stats") }} AS ftu_es
        ON
            dr.input_name = ftu_es.table_name
            AND dr.input_type = ftu_es.table_type

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

    WHERE dr.partition_date >= NOW() - INTERVAL 2 MONTH
)

SELECT
    *,
    {{ format_time('airflow_median_ready_time') }} AS airflow_median_ready_time_human,
    {{ format_time('ftu_median_ready_time') }} AS ftu_median_ready_time_human,
    {{ format_time('median_ready_time') }} AS median_ready_time_human,

    {{ format_time('airflow_task_effective_start_hours') }} AS airflow_task_effective_start_hours_human,
    {{ format_time('tables_effective_start_hours') }} AS tables_effective_start_hours_human,
    {{ format_time('effective_start_hours') }} AS effective_start_hours_human,

    effective_duration / p50_effective_duration AS slowness_ratio,
    (effective_duration - p50_effective_duration) * 60 AS slowdown_minutes,

    effective_duration * 60 AS effective_duration_minutes,
    p50_effective_duration * 60 AS p50_effective_duration_minutes,
    p80_effective_duration * 60 AS p80_effective_duration_minutes,
    CASE
        WHEN state = 'failed' THEN '999'
        ELSE COALESCE(ready_time_human, '999')
    END AS ready_time

FROM final_data
