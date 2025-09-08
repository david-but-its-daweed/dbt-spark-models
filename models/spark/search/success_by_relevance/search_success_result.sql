{{ config(
    schema = 'search',
    file_format = 'delta',
    materialized = 'incremental',
    incremental_strategy = 'insert_overwrite',
    partition_by = ['partition_date'],
    meta = {
        'model_owner': '@itangaev',
        'team': 'search',
        'bigquery_load': 'true'
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
),

join AS (
    SELECT
        a.*,
        a.event_date AS partition_date,  -- дата покупки
        b.text_relevance,
        COALESCE(b.text_relevance, CAST(a.category_relevance AS INT)) AS relevance,
        DATEDIFF(CURRENT_DATE(), CAST(a.search_date AS DATE)) <= 6 AS is_last_7_days
    FROM {{ ref('search_success_prepare_extracts') }} AS a
    LEFT JOIN markup AS b
        ON a.product_id = b.product_id
        AND a.query = b.query
        AND a.search_date = b.search_date
        AND a.event_date = b.partition_date
)

SELECT *
FROM join
DISTRIBUTE BY partition_date