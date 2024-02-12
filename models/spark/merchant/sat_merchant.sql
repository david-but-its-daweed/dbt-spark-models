{{ 
config(
    schema='merchant',
    materialized='view',
    meta = {
        'model_owner' : '@gburg',
        'bigquery_load': 'true',
    }
)
}}

SELECT
    merchant_id,
    created_time,
    updated_time,
    `createdBy`,
    `disabledBy`,
    activation_time,
    disabled_time,
    name,
    origin,
    enabled,
    activated_by_merchant,
    `disablingReason`,
    `disablingNote`,
    business_lines,
    category_ids,
    dbt_valid_from AS effective_ts,
    COALESCE(dbt_valid_to, CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS next_effective_ts
FROM {{ ref('scd2_mongo_merchant') }}