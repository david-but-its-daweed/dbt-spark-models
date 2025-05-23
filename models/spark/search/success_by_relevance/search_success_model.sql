{{ config(
    schema = 'search',
    materialized = 'table',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    meta = {
        'model_owner': '@itangaev',
        'team': 'search',
        'bigquery_load': 'false'
    }
) }}
WITH markup AS (
    SELECT
        partition_date,
        search_date,
        product_id,
        query,
        relevance AS text_relevance
    FROM {{ source('search', 'text_search_success_filter_marked_up') }}
)
SELECT
    a.search_date,
    a.event_date AS partition_date,  -- дата покупки
    a.device_id,
    a.query,
    a.search_category_id,
    a.search_type,
    a.product_id,
    a.os_type,
    a.device_country,
    a.has_open,
    a.has_to_cart,
    a.has_purchase,
    a.days_from_search_to_event,
    a.category_relevance,
    b.text_relevance,
    COALESCE(a.text_relevance, CAST(a.category_relevance AS INT)) AS relevance,
    DATEDIFF(CURRENT_DATE(), CAST(a.search_date AS DATE)) <= 6 AS is_last_7_days
FROM {{ ref('search_success_prepare_extracts') }} AS a
LEFT JOIN markup AS b
    ON a.product_id = b.product_id
    AND a.query = b.query
    AND a.search_date = b.search_date
    AND a.event_date = b.partition_date