{{
  config(
    materialized='table',
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