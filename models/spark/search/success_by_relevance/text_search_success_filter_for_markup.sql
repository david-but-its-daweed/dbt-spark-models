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
    search_date AS partition_date,
    query,
    product_id
FROM {{ ref('text_search_success_prepare_extracts') }}
WHERE
    query IS NOT NULL
    AND product_id IS NOT NULL
    AND has_purchase = 1
GROUP BY 1, 2, 3
ORDER BY 1
