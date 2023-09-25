{{
  config(
    materialized='table',
    alias='regions',
    schema='gold',
    file_format='delta',
    meta = {
        'model_owner' : '@gusev',
        'bigquery_load': 'true',
        'bigquery_overwrite': 'true',
        'bigquery_override_dataset_id': 'gold_migration',
    }
  )
}}

SELECT
    country_name,
    region_name,
    UPPER(country_code) AS country_code,
    region_name IN ('Europe', 'Africa', 'Other', 'CIS', 'LatAm') AS is_uniq,
    IF(UPPER(country_code) IN ('RU', 'DE', 'FR', 'MD', 'UA', 'GB', 'CH', 'ES', 'IT', 'BY'), UPPER(country_code), 'Other') AS top_country_code
FROM {{ ref('gold_regions_source') }}
