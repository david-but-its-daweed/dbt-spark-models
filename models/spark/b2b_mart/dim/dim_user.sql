{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}

WITH phone_number AS (
  SELECT DISTINCT 
    _id AS phone,
    uid AS user_id
  FROM {{ source('mongo', 'b2b_core_phone_credentials_daily_snapshot') }}
)

SELECT
  t.user_id,
  False AS is_anonymous,
  TIMESTAMP(t.created_ts_msk) AS created_ts_msk,
  TIMESTAMP(t.update_ts_msk) AS update_ts_msk,
  t.country,
  reject_reason.reason as reject_reason,
  key_validation_status.status as validation_status,
  t.owner_id AS owner_id,
  t.amo_crm_id,
  t.amo_id,
  t.invited_by_promo,
  t.is_partner as is_partner,
  t.first_name,
  t.last_name,
  t.middle_name,
  t.conversion_status,
  coalesce(t.is_test_user, FALSE) as is_joompro_employee,
  t.is_partner or coalesce(is_test_user, FALSE) as fake,
  fs.status as funnel_status,
  rr.reason as funnel_reject_reason,
  pt.type as partner_type,
  t.partner_source,
  t.email,
  pn.phone,
  t.reject_reason_partner,
  TIMESTAMP(t.dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(t.dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_user') }} t
left join {{ ref('key_funnel_status') }} fs on coalesce(cast(funnel_state.st as int), 0) = coalesce(cast(fs.id as int), 0)
left join {{ ref('key_validation_reject_reason') }} rr on cast(funnel_state.rjRsn as int)   = cast(rr.id as int)
left join {{ ref('key_partner_type') }} pt on cast(t.partner_type as int) = cast(pt.id as int)
left join {{ ref('key_validation_status') }} on key_validation_status.id = t.validation_status
left join {{ ref('key_validation_reject_reason') }} reject_reason on reject_reason.id = t.reject_reason
left join phone_number pn ON pn.user_id = t.user_id
where not is_test_user or is_test_user is null
