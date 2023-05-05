{{ config(
    schema='mongo',
    materialized='view',
) }}
select _id as merchant_id,
       createdTimeMs as created_time,
       updatedTimeMs as updated_time,
       activationTimeMs as activation_time,
       name,
       cast(origin as INTEGER) as origin,
       enabled,
       disablingReason,
       disablingNote
from {{ source('mongo', 'core_merchants_daily_snapshot') }}
