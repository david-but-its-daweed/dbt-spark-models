{{
  config(
    meta = {
      'model_owner' : '@gusev',
      'bigquery_load': 'true',
      'priority_weight': '1000',
    },
    materialized='table',
    file_format='delta',
  )
}}

SELECT
    card_bin,
    card_bank,
    card_brand,
    card_country,
    card_level,
    card_type
FROM
    {{ source('payments','card_bins') }}
