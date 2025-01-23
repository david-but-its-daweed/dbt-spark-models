{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='view'
) }}

WITH slo_tables AS (
    SELECT
        COLLECT_LIST(slo_id) AS slo_ids,
        table_name,
        table_type
    FROM {{ ref("slo_tables") }}
    GROUP BY table_name, table_type
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
        deps.dag_id,
        deps.task_id,
        deps.output_dag_id,
        deps.output_task_id,
        deps.partition_date
    FROM {{ ref('dependency_producers') }} AS deps
    LEFT JOIN slo_tables
        ON
            deps.output_name = slo_tables.table_name
            AND deps.output_type = slo_tables.table_type
),

airflow_data AS (
    SELECT *
    FROM {{ ref("airflow_task_instance_daily") }}
    WHERE run_number = 1
),

-- airflow_ftu_sensors AS (
--     SELECT
--         ti.partition_date,
--         ti.dag_id,
--         CASE
--             WHEN ti.task_id LIKE '%_bq_update_sensor' THEN 'bq'
--             ELSE 'spark'
--         END AS sensor_type,
--         CASE
--             WHEN ti.task_id LIKE '%_bq_update_sensor' THEN SUBSTR(ti.task_id, 1, LENGTH(ti.task_id) - 17)
--             ELSE SUBSTR(ti.task_id, 1, LENGTH(ti.task_id) - 14)
--         END AS sensor_table_name
--     FROM airflow_data AS ti
--     WHERE
--         ti.operator = 'HttpSensorAsync'
--         AND ti.task_id LIKE '%_bq_update_sensor'
-- ),

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
        MIN(IF(table_name LIKE 'mongo%', start_time, COALESCE(next_start_time, start_time))) AS start_date,
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

-- spark_updated_tables AS (
--     SELECT
--         table_full_name AS table_name,
--         'spark' AS table_type
--     FROM platform.beacon
--     WHERE create_datetime / 1000 >= UNIX_TIMESTAMP(DATE(NOW()) - INTERVAL 1 DAY)
-- ),

-- dates AS (
--     SELECT
--         dates.id,
--         dates.day_of_week_no
--     FROM mart.dim_date AS dates
--     WHERE
--         dates.id <= TO_DATE(NOW())
--         AND dates.id >= TO_DATE(NOW()) - INTERVAL 3 MONTH
-- ),

final_data AS (
    SELECT
        dependencies.source_id,
        dependencies.partition_date,
        (DAYOFWEEK(dependencies.partition_date) + 5) % 7 + 1 AS day_of_week_no,
        -- dates.day_of_week_no,
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
        ROUND(airflow_data.duration / 60) AS airflow_duration,
        ROUND(ftu.duration / 60) AS ftu_duration,
        ROUND(COALESCE(ftu.duration, airflow_data.duration) / 60) AS duration,
        airflow_data.try_number,
        COALESCE(ftu.table_name IS NOT NULL, FALSE) AS is_ftu_record,
        COALESCE(airflow_data.run_cnt = 1, FALSE) AS is_daily
        -- COALESCE(aftus.partition_date IS NOT NULL, FALSE) AS is_output_has_ftu_sensor,
        -- COALESCE(dependencies.input_type == 'spark' AND sut.table_name IS NULL, FALSE) AS is_not_updated_last_day
    FROM dependencies
    -- LEFT JOIN dates
    LEFT JOIN airflow_data
        ON
            dependencies.dag_id = airflow_data.dag_id
            AND dependencies.task_id = airflow_data.task_id
            AND dependencies.partition_date = airflow_data.partition_date
    LEFT JOIN ftu
        ON
            dependencies.input_name = ftu.table_name
            AND dependencies.input_type = ftu.platform
            AND dependencies.partition_date = ftu.partition_date
-- LEFT JOIN airflow_ftu_sensors AS aftus
--     ON
--         dependencies.output_dag_id = aftus.dag_id
--         AND dependencies.input_name = aftus.sensor_table_name
--         AND dependencies.input_type = aftus.sensor_type
--         AND dates.id = aftus.partition_date

-- LEFT JOIN spark_updated_tables AS sut
--     ON
--         dependencies.input_name = sut.table_name
--         AND dependencies.input_type = sut.table_type

-- WHERE
--     airflow_data.partition_date IS NOT NULL
)

SELECT
    *,
    (ready_time_hours + 3) % 24 AS ready_time_hours_msk,
    (start_time_hours + 3) % 24 AS start_time_hours_msk,
    {{ format_time('ready_time_hours') }} AS ready_time_human,
    {{ format_time('start_time_hours') }} AS start_time_human,
    {{ format_time('(ready_time_hours + 3) % 24') }} AS ready_time_human_msk,
    {{ format_time('(start_time_hours + 3) % 24') }} AS start_time_human_msk
    -- CONCAT_WS(':', LPAD(CAST(ready_time_hours AS INT), 2, '0'), LPAD(CAST((ready_time_hours % 1) * 60 AS INT), 2, '0')) AS ready_time_human,
    -- CONCAT_WS(':', LPAD(CAST(start_time_hours AS INT), 2, '0'), LPAD(CAST((start_time_hours % 1) * 60 AS INT), 2, '0')) AS start_time_human
    -- ROUND(ready_time_hours - 0.5) + ROUND((ready_time_hours - ROUND(ready_time_hours - 0.5)) * 60) / 100 AS ready_time_human,
    -- ROUND(start_time_hours - 0.5) + ROUND((start_time_hours - ROUND(start_time_hours - 0.5)) * 60) / 100 AS start_time_human
FROM final_data
