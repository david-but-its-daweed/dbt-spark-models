{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@n.rykov',
      'team': 'merchant',
    }
) }}

SELECT
    proposal_id,
    created_time,
    updated_time,
    product_id,
    merchant_id,
    status_history,
    target_variant_prices,
    cancel_info,
    will_be_cancelled_time
    TIMESTAMP(dbt_valid_from) AS effective_ts,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts
FROM {{ ref('scd2_mongo_joom_select_proposal') }}
