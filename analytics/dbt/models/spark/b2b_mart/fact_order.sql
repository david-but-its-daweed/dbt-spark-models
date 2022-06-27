{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics'
    }
) }}
SELECT
       order_id,
       created_ts_msk,
       ccy as user_ccy,
       delivery_time_days,
       friendly_id,
       request_id,
       linehaul_channel_id,
       device_id,
       user_id,
       reject_reason,
       last_order_status,
       dbt_valid_from as effective_ts_msk,
       dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_order') }} t