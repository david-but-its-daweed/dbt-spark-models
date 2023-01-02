{{ config(
    schema='mongo',
    materialized='view',
    meta = {
        priority_weight: 1000
    }
) }}
select _id as                                                         store_id,
       createdTimeMs                                               as created_time,
       updatedTimeMs                                               as updated_time,
       name,
       merchantId                                                  as merchant_id,
       specializationId                                            as specialization_id,
       isTop                                                       as is_top,
       isPower                                                     as is_power,
       enabled                                                     as enabledByJoom,
       enabled and enabledByMerchant and enabledFromMerchant as is_active,
       enabledByMerchant                                           as enabled_by_merchant,
       enabledFromMerchant                                         as enabled_from_merchant,
       enabled                                                     as enabled_by_joom,
       case
           when disablingReason = 1 then 'badPerformance'
           when disablingReason = 2 then 'counterfeit'
           when disablingReason = 3 then 'duplicates'
           when disablingReason = 4 then 'fraud'
           when disablingReason = 5 then 'other'
           else 'none'
           end                                                     as disabling_reason
from {{ source('mongo', 'core_stores_daily_snapshot') }}
