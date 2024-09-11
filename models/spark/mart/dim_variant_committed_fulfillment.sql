{{ config(
    schema='mart',
    materialized='view',
    meta = {
      'model_owner' : '@slava',
      'team': 'merchant',
    }
) }}

SELECT
    product_id,
    variant_id,
    cft,
    TIMESTAMP(dbt_valid_from) AS effective_ts,
    TIMESTAMP(dbt_valid_to) AS next_effective_ts
FROM {{ ref('scd2_mongo_variant_committed_fulfillment') }}
