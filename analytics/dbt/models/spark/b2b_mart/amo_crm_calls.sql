{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH first_call AS (
    SELECT 
        lead_id,
        MIN(call_ts_msk) AS first_call_ts_msk
    FROM {{ ref('scd2_mongo_amo_crm_calls') }}
    GROUP BY lead_id
)

SELECT
  call_id,
  call_status,
  call_ts_msk,
  first_call_ts_msk,
  owner_name,
  amo_contact_id,
  created_ts_msk,
  call_duration,
  t.lead_id,
  status,
  sub_status,
  status_priority,
  skorozvon_id,
  TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_amo_crm_calls') }} t
LEFT JOIN first_call f ON t.lead_id = f.lead_id
