{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov'
    },
) }}
SELECT
    _id AS merchant_id,
    createdTimeMs AS created_time,
    updatedTimeMs AS updated_time,
    activationTimeMs AS activation_time,
    name,
    CAST(origin AS INTEGER) AS origin,
    activatedByMerchant AS activated_by_merchant,
    enabled,
    blockActionTime AS disabled_time,
    disablingReason AS disabling_reason,
    disablingNote AS disabling_note,
    businessLines AS business_lines,
    categoryids AS category_ids
FROM {{ source('mongo', 'core_merchants_daily_snapshot') }}
