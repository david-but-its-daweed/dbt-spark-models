{{
  config(
    materialized='table',
    alias='countries',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@analytics.duty',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}


WITH countries_properties AS (
    SELECT
        country_code,
        currency_code,
        country_priority_type,
        macroregion_name,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY currency_code) AS row_num
    FROM {{ ref('countries_properties') }}
)

SELECT DISTINCT
    gr.country_code,
    gr.country_name,
    IF(
        MAX(gr.is_uniq) OVER (PARTITION BY gr.country_code) = TRUE,
        FIRST_VALUE(gr.region_name) OVER (PARTITION BY gr.country_code ORDER BY gr.is_uniq DESC),
        'Other'
    ) AS region_name,
    COALESCE(gr.top_country_code, 'Other') AS top_country_code,
    cc.currency_code AS national_currency_code,
    COALESCE(cc.country_priority_type, 'Other') AS country_priority_type,
    COALESCE(cc.macroregion_name, 'Other') AS macroregion_name
FROM {{ ref('gold_regions') }} AS gr
LEFT JOIN countries_properties AS cc
    ON
        gr.country_code = cc.country_code
        AND cc.row_num = 1
