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
        SIZE(`input`.`input_path`) AS input_rank
    FROM {{ source('platform', 'table_dependencies_v2') }}
    WHERE
        TRUE
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
        partition_date,
        `table`.tableName AS table_name,
        `table`.tableType AS table_type,
        airflow_task.dagId AS dag_id,
        airflow_task.taskId AS task_id
    FROM {{ source('platform', 'table_producers') }}
    WHERE partition_date = (SELECT MAX(tp.partition_date) FROM {{ source('platform', 'table_producers') }} AS tp)
)

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
    output_producer_tasks.task_id AS output_task_id
FROM deps
LEFT JOIN producer_tasks AS input_producer_tasks
    ON
        deps.input_name = input_producer_tasks.table_name
        AND deps.input_type = input_producer_tasks.table_type
LEFT JOIN producer_tasks AS output_producer_tasks
    ON
        deps.output_name = output_producer_tasks.table_name
        AND deps.output_type = output_producer_tasks.table_type
