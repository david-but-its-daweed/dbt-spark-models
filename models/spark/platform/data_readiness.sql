{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

WITH deps AS (
    SELECT
        output.tableName AS output_name,
        output.tableType AS output_type,
        `input`.`table`.tableName AS input_name,
        `input`.`table`.tableType AS input_type,
        `input`.`table`.isSeed AS input_is_seed,
        `input`.`input_path` AS input_path,
        SIZE(`input`.`input_path`) AS input_rank
    FROM platform.table_dependencies_v2
    WHERE
        NOT `input`.`table`.isSeed AND (`input`.`table`.`tableName`, `input`.`table`.`tableType`)
        NOT IN (
            SELECT
                mt.full_table_name,
                mt.type
            FROM {{ ref("manual_tables") }} AS mt
        )
),

slo_tables AS (
    SELECT
        COLLECT_LIST(slo_id) AS slo_ids,
        table_name,
        table_type
    FROM {{ ref("slo_tables") }}
    GROUP BY table_name, table_type
),

producer_tasks AS (
    SELECT
        `table`.tableName AS table_name,
        `table`.tableType AS table_type,
        airflow_task.dagId AS dag_id,
        airflow_task.taskId AS task_id
    FROM platform.table_producers
),

dependencies AS (
    SELECT
        deps.output_name,
        deps.output_type,
        deps.input_name,
        deps.input_type,
        deps.input_rank,
        deps.input_path,
        EXPLODE(ARRAY_UNION(
            COALESCE(slo_tables.slo_ids, ARRAY()),
            ARRAY(deps.output_name || '_' || deps.output_type)
        )) AS source_id,
        producer_tasks.dag_id,
        producer_tasks.task_id,
        output_producer_tasks.dag_id AS output_dag_id,
        output_producer_tasks.task_id AS output_task_id
    FROM deps
    LEFT JOIN slo_tables ON deps.output_name = slo_tables.table_name AND deps.output_type = slo_tables.table_type
    LEFT JOIN producer_tasks
        ON
            deps.input_name = producer_tasks.table_name
            AND deps.input_type = producer_tasks.table_type
    LEFT JOIN producer_tasks AS output_producer_tasks
        ON
            deps.output_name = output_producer_tasks.table_name
            AND deps.output_type = output_producer_tasks.table_type
),

airflow_data AS (
    SELECT *
    FROM {{ ref("airflow_task_instance_daily") }}
    WHERE run_number = 1
),

airflow_ftu_sensors AS (
    SELECT
        ti.partition_date,
        ti.dag_id,
        CASE
            WHEN ti.task_id LIKE '%_bq_update_sensor' THEN 'bq'
            ELSE 'spark'
        END AS sensor_type,
        CASE
            WHEN ti.task_id LIKE '%_bq_update_sensor' THEN SUBSTR(ti.task_id, 1, LENGTH(ti.task_id) - 17)
            ELSE SUBSTR(ti.task_id, 1, LENGTH(ti.task_id) - 14)
        END AS sensor_table_name
    FROM airflow_data AS ti
    WHERE
        ti.operator = 'HttpSensorAsync'
        AND ti.task_id LIKE '%_bq_update_sensor'
),

ftu AS (
    SELECT
        platform,
        table_name,
        partition_date,
        start_date,
        end_date,
        duration
    FROM {{ ref("ftu_archive") }}
    WHERE
        partition_date < TO_DATE(NOW()) - INTERVAL 1 DAY
        AND partition_date >= TO_DATE(NOW()) - INTERVAL 3 MONTH

    UNION ALL

    SELECT
        platform,
        table_name,
        TO_DATE(CASE
            WHEN HOUR(start_time) >= 22 THEN DATE_TRUNC('Day', start_time + INTERVAL 24 HOURS)
            ELSE DATE_TRUNC('Day', start_time)
        END) AS partition_date,
        MIN(start_time) AS start_date,
        MIN(dttm) AS end_date,
        UNIX_TIMESTAMP(MIN(dttm)) - UNIX_TIMESTAMP(MIN(start_time)) AS duration
    FROM platform.fact_table_update
    WHERE
        start_time >= DATE_SUB(TO_DATE(NOW()), 1) - INTERVAL 2 HOUR
    GROUP BY
        platform,
        table_name,
        TO_DATE(CASE
            WHEN HOUR(start_time) >= 22 THEN DATE_TRUNC('Day', start_time + INTERVAL 24 HOURS)
            ELSE DATE_TRUNC('Day', start_time)
        END)
),

final_data AS (
    SELECT
        dependencies.source_id,
        airflow_data.partition_date,
        dates.day_of_week_no,
        airflow_data.run_id,
        dependencies.input_name,
        dependencies.input_type,
        dependencies.input_rank,
        dependencies.input_path,
        dependencies.dag_id,
        dependencies.task_id,
        dependencies.output_name,
        dependencies.output_type,
        dependencies.output_dag_id,
        dependencies.output_task_id,
        dependencies.input_name || '_' || dependencies.input_type AS input_full_name,
        (UNIX_TIMESTAMP(COALESCE(ftu.end_date, airflow_data.end_date)) - UNIX_TIMESTAMP(DATE(COALESCE(ftu.end_date, airflow_data.end_date)))) / 60 / 60 AS ready_time_hours,
        (UNIX_TIMESTAMP(COALESCE(ftu.start_date, airflow_data.start_date)) - UNIX_TIMESTAMP(DATE(COALESCE(ftu.start_date, airflow_data.start_date)))) / 60 / 60 AS start_time_hours,
        dependencies.input_rank || '_' || dependencies.input_name || '_' || dependencies.input_type AS input_table,
        CASE WHEN ftu.end_date IS NOT NULL THEN 'success' ELSE airflow_data.state END AS state,
        airflow_data.priority_weight,
        ftu.start_date AS ftu_start_date,
        ftu.end_date AS ftu_end_date,
        airflow_data.start_date AS airflow_start_date,
        airflow_data.end_date AS airflow_end_date,
        COALESCE(ftu.start_date, airflow_data.start_date) AS start_date,
        COALESCE(ftu.end_date, airflow_data.end_date) AS end_date,
        airflow_data.duration AS airflow_duration,
        ftu.duration AS ftu_duration,
        COALESCE(ftu.duration, airflow_data.duration) AS duration,
        airflow_data.try_number,
        COALESCE(airflow_data.run_cnt = 1, FALSE) AS is_daily,
        COALESCE(aftus.partition_date IS NOT NULL, FALSE) AS is_output_has_ftu_sensor
    FROM dependencies
    LEFT JOIN mart.dim_date AS dates
    LEFT JOIN airflow_data
        ON
            dependencies.dag_id = airflow_data.dag_id
            AND dependencies.task_id = airflow_data.task_id
            AND dates.id = airflow_data.partition_date
    LEFT JOIN ftu
        ON
            dependencies.input_name = ftu.table_name
            AND dependencies.input_type = ftu.platform
            AND dates.id = ftu.partition_date
    LEFT JOIN airflow_ftu_sensors AS aftus
        ON
            dependencies.output_dag_id = aftus.dag_id
            AND dependencies.input_name = aftus.sensor_table_name
            AND dependencies.input_type = aftus.sensor_type
            AND dates.id = aftus.partition_date
    WHERE
        dates.id <= TO_DATE(NOW())
        AND dates.id >= TO_DATE(NOW()) - INTERVAL 6 MONTH
)

SELECT
    *,
    {{ format_time('ready_time_hours') }} AS ready_time_human,
    {{ format_time('start_time_hours') }} AS start_time_human
    -- CONCAT_WS(':', LPAD(CAST(ready_time_hours AS INT), 2, '0'), LPAD(CAST((ready_time_hours % 1) * 60 AS INT), 2, '0')) AS ready_time_human,
    -- CONCAT_WS(':', LPAD(CAST(start_time_hours AS INT), 2, '0'), LPAD(CAST((start_time_hours % 1) * 60 AS INT), 2, '0')) AS start_time_human
    -- ROUND(ready_time_hours - 0.5) + ROUND((ready_time_hours - ROUND(ready_time_hours - 0.5)) * 60) / 100 AS ready_time_human,
    -- ROUND(start_time_hours - 0.5) + ROUND((start_time_hours - ROUND(start_time_hours - 0.5)) * 60) / 100 AS start_time_human
FROM final_data
