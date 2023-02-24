{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}
SELECT
  user_id,
  anon AS is_anonymous,
  TIMESTAMP(created_ts_msk) AS created_ts_msk,
  TIMESTAMP(update_ts_msk) AS update_ts_msk,
  pref_country,
  reject_reason,
  validation_status,
  owner_id AS owner_id,
  amo_crm_id,
  amo_id,
  TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_user') }} t
