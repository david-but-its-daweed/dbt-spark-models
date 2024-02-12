{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov'
    },
) }}
SELECT
    _id AS merchant_id,
    "createdTimeMs" AS created_time,
    "updatedTimeMs" AS updated_time,
    "activationTimeMs" AS activation_time,
    name,
    CAST(origin AS INTEGER) AS origin,
    "activatedByMerchant" AS activated_by_merchant,
    enabled,
    "blockActionTime" AS disabled_time,
    "disablingReason",
    "disablingNote",
    "businessLines" AS business_lines,
    "categoryIds" AS category_ids,
    "createdBy",
    "disabledBy"
FROM {{ source('mongo', 'core_merchants_daily_snapshot') }}
