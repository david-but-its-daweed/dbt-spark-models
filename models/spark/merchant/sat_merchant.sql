{{ 
config(
    schema='merchant',
    materialized='view',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
    }
)
}}

SELECT
    merchant_id,
    created_time,
    updated_time,
    created_by,
    disabled_by,
    activation_time,
    disabled_time,
    name,
    origin,
    enabled,
    activated_by_merchant,
    disabling_reason,
    disabling_note,
    business_lines,
    category_ids,
    lead,
    lead_notes,
    dbt_valid_from AS effective_ts,
    COALESCE(dbt_valid_to, CAST('9999-12-31 23:59:59' AS TIMESTAMP)) AS next_effective_ts
FROM {{ ref('scd2_mongo_merchant') }}
