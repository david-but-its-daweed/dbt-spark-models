{{ config(
    schema = 'search',
    file_format = 'delta',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = ['partition_date'],
    meta = {
      'model_owner': '@itangaev',
      'team': 'search',
      'bigquery_load': 'false'
    }
) }}

SELECT
    a.event_date AS partition_date,  -- дата покупки
    a.search_date,
    a.query,
    a.product_id,
    a.searchRequestId
FROM {{ ref('search_success_prepare_extracts') }} AS a
WHERE
    a.query IS NOT NULL
    AND a.product_id IS NOT NULL
    AND a.has_purchase = 1
    AND a.search_type = 'text_search'
ORDER BY 1
