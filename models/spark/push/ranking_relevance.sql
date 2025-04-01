{{
  config(
    incremental_strategy='insert_overwrite',
    materialized = 'incremental',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    alias = 'ranking_relevance',
    schema = 'push',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true'
    }
  )
}}

SELECT
    partition_date,
    version,
    ROUND(AVG(relevance), 3) AS avg_relevance,
    ROUND(PERCENTILE_APPROX(relevance, 0.1), 3) AS percentile_10_relevance,
    ROUND(PERCENTILE_APPROX(relevance, 0.5), 3) AS percentile_50_relevance,
    ROUND(PERCENTILE_APPROX(relevance, 0.9), 3) AS percentile_90_relevance,
    COUNT(*) AS observations
FROM {{ source('push', 'user_campaign_relevance') }}
WHERE
    1 = 1
    {% if is_incremental() %}
        AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
    {% else %}
        AND partition_date >= DATE("2025-01-01")
    {% endif %}
GROUP BY 1, 2