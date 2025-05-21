{{ config(
    schema='search',
    materialized='table',
    file_format='parquet',
    partition_by=['partition_date'],
    meta = {
      'model_owner' : '@itangaev',
      'team': 'search',
      'bigquery_load': 'false'
    }
) }}
SELECT
    event_date AS partition_date,  -- дата покупки
    search_date,
    query,
    product_id,
    searchRequestId
FROM {{ ref('search_success_prepare_extracts') }}
WHERE
    query IS NOT NULL
    AND product_id IS NOT NULL
    AND has_purchase = 1
    AND search_type = "text_search"
ORDER BY 1
