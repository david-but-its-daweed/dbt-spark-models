{{ config(
    schema='mongo',
    materialized='view',
) }}
select _id as                  experiment_id,
       updatedTimeMs        as updated_time,
       createdTimeMs        as created_time,
       type,
       enabled,
       startTimeMs          as start_time,
       endTimeMs            as end_time,
       criticalUpdateTimeMs as critical_update_time
from {{ source('mongo', 'core_experiments_daily_snapshot') }}