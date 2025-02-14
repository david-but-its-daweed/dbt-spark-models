{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'false'
    }
) }}

SELECT _id AS user_id,
    fn as first_name,
    ln as last_name,
    mn as middle_name,
    conversionStatus as conversion_status,
    funnelState as funnel_state,
    country,
    valSt.ohRsn AS hold_reason,
    valSt.rjRsn AS reject_reason,
    valSt.st AS validation_status,
    moderator_id as owner_id,
    amoCrmId AS amo_crm_id,
    amoId AS amo_id,
    invitedByPromo as invited_by_promo,
    invitedTime as invited_time,
    case when partnerState.type != 'NoStatus' then true else false end as is_partner,
    isTestUser as is_test_user,
    partnerSource as partner_source,
    partnerState as partner_state,
    partnerState.type as partner_type,
    partnerState.rjRsn as reject_reason_partner,
    millis_to_ts_msk(ctms) AS created_ts_msk,
    millis_to_ts_msk(utms) AS update_ts_msk,
    email,
    phone,
    dbt_scd_id,
    dbt_updated_at,
    dbt_valid_from,
    dbt_valid_to
FROM {{ ref('scd2_users_snapshot') }}
