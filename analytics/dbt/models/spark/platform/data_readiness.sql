{{ config(
    schema='platform',
    materialized='view'
) }}

with deps as (SELECT output.tableName       as output_name,
                     output.tableType       as output_type,
                     input.table.tableName  as input_name,
                     input.table.tableType  as input_type,
                     input.input_path       as input_path,
                     size(input.input_path) as input_rank
              FROM platform.table_dependencies_v2),

     slo_tables as (SELECT collect_list(slo_id) as slo_ids,
                           table_name,
                           table_type
                    from platform.slo_tables
                    group by table_name, table_type),

     producer_tasks as (select table.tableName     as table_name,
                               table.tableType     as table_type,
                               airflow_task.dagId  as dag_id,
                               airflow_task.taskId as task_id
                        from platform.table_producers),

     dependencies as (select output_name,
                             output_type,
                             input_name,
                             input_type,
                             input_rank,
                             input_path,
                             explode(array_union(coalesce(slo_tables.slo_ids, array()),
                                                 array(output_name || '_' || output_type))) as source_id,
                            producer_tasks.dag_id,
                            producer_tasks.task_id
                      FROM deps
                               left join slo_tables on slo_tables.table_name = deps.output_name and slo_tables.table_type = deps.output_type
                               left join producer_tasks
                                    on producer_tasks.table_name = deps.input_name
                                    and producer_tasks.table_type = deps.input_type),

     airflow_data as (SELECT task_id,
                             dag_id,
                             run_id,
                             start_date,
                             end_date
                      FROM platform.airflow_task_instance_archive
                      where start_date < to_date(NOW()) - interval 1 day
                        and start_date > NOW() - interval 2 month

                      UNION ALL

                      SELECT task_id,
                             dag_id,
                             run_id,
                             min(start_date) as start_date,
                             min(end_date)   as end_date

                      FROM platform.airflow_task_instance
                      where start_date >= to_date(NOW()) - interval 1 day
                        and state = 'success'
                        and start_date > NOW() - interval 2 month
                      group by task_id, dag_id, run_id),

     data as (select task_id,
                     dag_id,
                     to_date(CASE
                                 WHEN hour(start_date) >= 22 THEN date_trunc('Day', start_date)
                                 ELSE date_trunc('Day', start_date) - interval 24 hours
                         END)   as partition_date,
                     start_date as start_date,
                     end_date   as end_date
              from airflow_data
              where start_date > NOW() - interval 2 month)

select source_id,
       dates.id as date,
       partition_date,
       dependencies.input_name,
       dependencies.input_type,
       dependencies.input_rank,
       dependencies.input_path,
       dependencies.dag_id,
       dependencies.task_id,
       dependencies.input_name || '_' || dependencies.input_type                                   as input_full_name,
       (unix_timestamp(end_date) - unix_timestamp(partition_date)) / 60 / 60 - 24                  as ready_time_hours,
       dependencies.input_rank || '_' || dependencies.input_name || '_' || dependencies.input_type as input_table,
       start_date,
       end_date
from dependencies
         left join mart.dim_date as dates
         left join data on dependencies.dag_id = data.dag_id
                and dependencies.task_id = data.task_id
                and dates.id = data.partition_date
