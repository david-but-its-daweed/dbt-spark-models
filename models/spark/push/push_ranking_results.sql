{{
  config(
    incremental_strategy='insert_overwrite',
    materialized = 'incremental',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    alias = 'push_ranking_results',
    schema = 'push',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true'
    }
  )
}}



WITH ranking_stg AS (
    SELECT
        partition_date,
        user_id,
        payload.pushRankingRequestId AS ranking_id,
        payload.modelVersion AS model_version,
        EXPLODE(payload.pushRequests) AS col
    FROM {{ source('mart', 'device_events') }}
    WHERE
        type = "pushRanked"
        AND ephemeral = FALSE
        {% if is_incremental() %}
        AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-01-01")
        {% endif %}
),

ranking_2 AS (
    SELECT
        partition_date,
        ranking_id,
        model_version,
        col.requestId AS request_id,
        col.requestType AS request_type,
        IF(col.isPrime, 1000, col.weight) AS weight,
        FIRST_VALUE(col.requestType) OVER (PARTITION BY user_id, ranking_id ORDER BY IF(col.isPrime, 1000, col.weight) DESC) AS winner_type,
        AVG(IF(col.isPrime, 1000, col.weight)) OVER (PARTITION BY partition_date, col.requestType) AS avg_weight
    FROM ranking_stg
),

ranking AS (
    SELECT
        partition_date,
        ranking_id,
        model_version,
        request_id,
        request_type,
        weight,
        winner_type,
        avg_weight,
        MAX(IF(winner_type = request_type, avg_weight, NULL)) OVER (PARTITION BY partition_date, winner_type) AS avg_weight_winner
    FROM ranking_2
)


SELECT
    partition_date,
    model_version,
    request_type,
    winner_type,
    ROUND(MAX(avg_weight), 3) AS avg_push_type_weight,
    ROUND(MAX(avg_weight_winner), 3) AS avg_winner_push_type_weight,
    COUNT(DISTINCT ranking_id) AS rankings
FROM ranking
GROUP BY 1, 2, 3, 4