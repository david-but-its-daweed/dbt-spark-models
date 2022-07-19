{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true'
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
  owner_id as owner_id,
  dbt_valid_from as effective_ts_msk,
  dbt_valid_to as next_effective_ts_msk
from {{ ref('scd2_mongo_user') }} t