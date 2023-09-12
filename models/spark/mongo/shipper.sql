{{ config(
    meta = {
      'model_owner' : '@leonid.enov'
    },
    schema='mongo',
    materialized='view',
) }}
select _id as           shipper_id,
       createdTimeMs as created_time,
       updatedTimeMs as updated_time,
       name
from {{ source('mongo', 'core_tracking_providers_daily_snapshot') }}