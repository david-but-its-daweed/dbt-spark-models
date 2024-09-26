{{ config(
    schema='category_management',
    materialized='table',
    meta = {
      'model_owner' : '@catman-analytics.duty',
      'team': 'category_management',
      'bigquery_load': 'true',
    }
) }}


-- коды: https://www.notion.so/joomteam/Diagnostics-Origins-Codes-11eb1f1bbd3645d5b65858057b20e47a

SELECT DISTINCT
    product_id,
    d.code AS diag_code
FROM (
    SELECT
        _id AS product_id,
        EXPLODE(diags) AS d
    FROM {{ source('mongo', 'product_product_diagnoses_daily_snapshot') }}
)
WHERE d.code IN ('J1178')
