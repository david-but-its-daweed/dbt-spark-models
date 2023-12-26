{{ config(
    schema='mongo',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov'
    },
) }}
SELECT
    _id AS merchant_id,
    createdtimems AS created_time,
    updatedtimems AS updated_time,
    activationtimems AS activation_time,
    name,
    CAST(origin AS INTEGER) AS origin,
    activatedbymerchant AS activated_by_merchant,
    enabled,
    blockactiontime AS disabled_time,
    disablingreason,
    disablingnote,
    businesslines
FROM {{ source('mongo', 'core_merchants_daily_snapshot') }}
