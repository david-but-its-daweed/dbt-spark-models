{{
  config(
    materialized='table',
    alias='regions',
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

SELECT
    country_name,
    region_name,
    UPPER(country_code) AS country_code,
    region_name IN ('Europe', 'Africa', 'Other', 'CIS', 'LatAm') AS is_uniq,
    IF(UPPER(country_code) IN (
        'RU',
        'DE',
        'UA',
        'MD',
        'CH',
        'SE',
        'FR',
        'PL',
        'GB',
        'IL',
        'NL',
        'NO',
        'AT',
        'BE',
        'RO',
        'ES',
        'KZ',
        'IT',
        'CZ',
        'HU',  -- the last country with GMV share > 1%
        'GR',
        'LT',
        'PT',
        'EE'
    ), UPPER(country_code), 'Other') AS top_country_code  -- countries with GMV share > 0.5% of total
FROM {{ ref('gold_regions_source') }}
