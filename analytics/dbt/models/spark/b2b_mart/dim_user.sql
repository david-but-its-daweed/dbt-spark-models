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
  is_partner as is_partner,
  first_name,
  last_name,
  middle_name,
  conversion_status,
  coalesce(is_test_user, FALSE) as is_joompro_employee,
  is_partner or coalesce(is_test_user, FALSE) as fake,
  fs.status as funnel_status,
  rr.reason as funnel_reject_reason,
  pt.type as partner_type,
  partner_source,
  TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
  TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_user') }} t
left join {{ ref('key_funnel_status') }} fs on coalesce(cast(funnel_state.st as int), 0) = coalesce(cast(fs.id as int), 0)
left join {{ ref('key_validation_reject_reason') }} rr on cast(funnel_state.rjRsn as int) = cast(rr.id as int)
left join {{ ref('key_partner_type') }} pt on cast(t.partner_type as int) = cast(pt.id as int)
where not is_test_user or is_test_user is null
