{{ config(
    schema='merchant',
    materialized='view',
    meta = {
      'model_owner' : '@mshlyapenko',
      'team': 'merchant',
    }
) }}
SELECT
    id,
    type,
    merchant_id,
    method_id,
    status,
    error_reasons,
    updated_time,
    TIMESTAMP(dbt_valid_from) AS effective_ts_msk,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts_msk
FROM {{ ref('scd2_mongo_merchant_payment_channel') }} t
