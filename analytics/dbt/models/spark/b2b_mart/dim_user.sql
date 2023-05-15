{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH requests AS (
    SELECT
        user_id,
        MAX(is_joompro_employee) AS is_joompro_employee
    FROM {{ ref('fact_user_request') }}
    group by user_id
)

SELECT
  t.user_id,
  False AS is_anonymous,
  TIMESTAMP(created_ts_msk) AS created_ts_msk,
  TIMESTAMP(update_ts_msk) AS update_ts_msk,
  country,
  reject_reason,
  validation_status,
  owner_id AS owner_id,
  amo_crm_id,
  amo_id,
  invited_by_promo,
  isPartner as is_partner,
  first_name,
  last_name,
  middle_name,
  conversion_status,
  coalesce(is_joompro_employee, FALSE) as is_joompro_employee,
  is_partner or coalesce(is_joompro_employee, FALSE) as fake,
  TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_user') }} t
left join requests as r on t.user_id = r.user_id
where is_partner is null
