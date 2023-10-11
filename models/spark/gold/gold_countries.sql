{{
  config(
    materialized='table',
    alias='countries',
    schema='gold',
    file_format='parquet',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'priority_weight': '1000',
    }
  )
}}


WITH countries_currencies AS (
    SELECT
        country_code,
        currency_code,
        ROW_NUMBER() OVER (PARTITION BY country_code ORDER BY currency_code) AS row_num
    FROM {{ ref('countries_currencies') }}
)

SELECT DISTINCT
    gr.country_code,
    gr.country_name,
    IF(
        MAX(gr.is_uniq) OVER (PARTITION BY gr.country_code) = TRUE,
        FIRST_VALUE(gr.region_name) OVER (PARTITION BY gr.country_code ORDER BY gr.is_uniq DESC),
        'Other'
    ) AS region_name,
    gr.top_country_code,
    cc.currency_code AS national_currency_code
FROM {{ ref('gold_regions') }} AS gr
LEFT JOIN countries_currencies AS cc ON cc.country_code = gr.country_code AND cc.row_num = 1
