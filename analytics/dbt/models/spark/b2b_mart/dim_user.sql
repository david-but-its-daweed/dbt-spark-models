{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics'
    }
) }}
SELECT
  user_id,
  anon AS is_anonymous,
  created_ts_msk,
  update_ts_msk,
  pref_country,
  reject_reason,
  validation_status,
  staff AS is_joompro_employee,
  dbt_valid_from as effective_ts_msk,
  dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_user') }} t