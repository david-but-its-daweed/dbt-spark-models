{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table'
) 
}}

WITH deps AS (
    SELECT
        output.tableName AS output_name,
        output.tableType AS output_type,
        output.isSeed AS output_is_seed,
        `input`.`table`.tableName AS input_name,
        `input`.`table`.tableType AS input_type,
        `input`.`table`.isSeed AS input_is_seed,
        `input`.`input_path` AS input_path,
        SIZE(`input`.`input_path`) AS input_rank,
        partition_date
    FROM {{ source('platform', 'table_dependencies_v2') }}
    WHERE
        TRUE
        -- AND partition_date = (SELECT MAX(td.partition_date) FROM {{ source('platform', 'table_dependencies_v2') }} AS td)
        AND partition_date >= TO_DATE(NOW()) - INTERVAL 3 MONTH
        AND NOT `input`.`table`.isSeed
        -- AND NOT (`input`.`table`.tableName = output.tableName AND `input`.`table`.tableType = output.tableType)
        AND (`input`.`table`.`tableName`, `input`.`table`.`tableType`)
        NOT IN (
            SELECT
                mt.full_table_name,
                mt.type
            FROM {{ ref("manual_tables") }} AS mt
        )
),

producer_tasks AS (
    SELECT
        `table`.tableName AS table_name,
        `table`.tableType AS table_type,
        airflow_task.dagId AS dag_id,
        airflow_task.taskId AS task_id,
        partition_date
        -- MAX(partition_date) AS last_observed_time
    FROM {{ source('platform', 'table_producers') }}
    WHERE partition_date >= TO_DATE(NOW()) - INTERVAL 3 MONTH
    -- GROUP BY `table`, airflow_task
),

dependencies AS (
    SELECT
        deps.input_name,
        deps.input_type,
        deps.input_rank,
        deps.input_path,
        input_producer_tasks.dag_id,
        input_producer_tasks.task_id,

        deps.output_name,
        deps.output_type,
        output_producer_tasks.dag_id AS output_dag_id,
        output_producer_tasks.task_id AS output_task_id,
        DATE_ADD(deps.partition_date, 1) AS partition_date
    FROM deps
    LEFT JOIN producer_tasks AS input_producer_tasks
        ON
            deps.input_name = input_producer_tasks.table_name
            AND deps.input_type = input_producer_tasks.table_type
            AND deps.partition_date = input_producer_tasks.partition_date
    LEFT JOIN producer_tasks AS output_producer_tasks
        ON
            deps.output_name = output_producer_tasks.table_name
            AND deps.output_type = output_producer_tasks.table_type
            AND deps.partition_date = output_producer_tasks.partition_date
),

dependencies_future AS (
    SELECT
        *,
        EXPLODE(ARRAY(1, 2, 3, 4, 5, 6, 7)) AS days_inc
    FROM dependencies
    WHERE partition_date = (SELECT MAX(d.partition_date) FROM dependencies AS d)
)

SELECT
    input_name,
    input_type,
    input_rank,
    input_path,
    dag_id,
    task_id,
    output_name,
    output_type,
    output_dag_id,
    output_task_id,
    partition_date
FROM
    dependencies

UNION ALL

SELECT
    input_name,
    input_type,
    input_rank,
    input_path,
    dag_id,
    task_id,
    output_name,
    output_type,
    output_dag_id,
    output_task_id,
    DATE_ADD(partition_date, days_inc) AS partition_date
FROM
    dependencies_future


