{{
  config(
    incremental_strategy='insert_overwrite',
    materialized = 'incremental',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    alias = 'ranking_funnel',
    schema = 'push',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true'
    }
  )
}}

WITH notifications AS (
    SELECT
        user_id,
        partition_date,
        payload.type AS push_type,
        payload.notificationRequestId AS request_id
    FROM {{ source('mart', 'device_events') }}
    WHERE
        type = "notificationRequested"
        AND ephemeral = FALSE
        {% if is_incremental() %}
            AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-01-01")
        {% endif %}
),

stats_notification_request AS (
    SELECT
        push_type,
        partition_date,
        COUNT(request_id) * 20 AS notification_requests
    FROM notifications
    GROUP BY 1, 2
),

ranking_stg AS (
    SELECT
        user_id,
        partition_date,
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

ranking AS (
    SELECT
        user_id,
        partition_date,
        ranking_id,
        model_version,
        col.requestId AS request_id,
        col.requestType AS push_type,
        IF(col.isPrime, 1000, col.weight) AS weight,
        ROW_NUMBER() OVER (PARTITION BY user_id, ranking_id ORDER BY IF(col.isPrime, 1000, col.weight) DESC) AS rn
    FROM ranking_stg
),

stat_ranking AS (
    SELECT
        partition_date,
        push_type,
        ROUND(AVG(IF(model_version = "ctr_v9_extended", weight, NULL)), 3) AS avg_weight_ctr_v9_extended,
        ROUND(AVG(IF(model_version = "q_touch_v1", weight, NULL)), 3) AS avg_weight_q_touch_v1,
        COUNT(DISTINCT IF(model_version = "ctr_v9_extended", ranking_id, NULL)) AS ranking_cnt_ctr_v9_extended,
        COUNT(DISTINCT IF(model_version = "q_touch_v1", ranking_id, NULL)) AS ranking_cnt_q_touch_v1,
        COUNT(DISTINCT IF(model_version = "ctr_v9_extended", request_id, NULL)) AS requests_cnt_ctr_v9_extended,
        COUNT(DISTINCT IF(model_version = "q_touch_v1", request_id, NULL)) AS requests_cnt_q_touch_v1,
        ROUND(COUNT(DISTINCT request_id) / COUNT(DISTINCT user_id), 3) AS avg_requests_per_user
    FROM ranking
    GROUP BY 1, 2
),

push_sent AS (
    SELECT
        user_id,
        partition_date,
        payload.type AS push_type,
        payload.notificationRequestId AS request_id
    FROM {{ source('mart', 'device_events') }}
    WHERE
        type = "pushSent"
        AND ephemeral = FALSE
        {% if is_incremental() %}
            AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-01-01")
        {% endif %}
),

sent_ranking AS (
    SELECT
        partition_date,
        push_type,
        COUNT(DISTINCT request_id) AS sents
    FROM push_sent
    GROUP BY 1, 2
),

funnel AS (
    SELECT
        n.partition_date,
        n.push_type,
        ROUND(COUNT(DISTINCT r.request_id) / COUNT(DISTINCT n.request_id), 3) AS request_to_ranked,
        ROUND(COUNT(DISTINCT s.request_id) / COUNT(DISTINCT r.request_id), 3) AS ranked_to_sent,
        ROUND(COUNT(DISTINCT s.request_id) / COUNT(DISTINCT n.request_id), 3) AS request_to_sent,
        ROUND(
            COUNT(DISTINCT IF(r.model_version = "ctr_v9_extended", s.request_id, NULL)) / COUNT(DISTINCT IF(r.model_version = "ctr_v9_extended", r.request_id, NULL)), 3
        ) AS ranked_to_sent_v9_extended,
        ROUND(COUNT(DISTINCT IF(r.model_version = "q_touch_v1", s.request_id, NULL)) / COUNT(DISTINCT IF(r.model_version = "q_touch_v1", r.request_id, NULL)), 3) AS ranked_to_sent_q_touch_v1
    FROM notifications AS n
    LEFT JOIN ranking AS r
        ON
            n.user_id = r.user_id
            AND n.request_id = r.request_id
            AND n.push_type = r.push_type
            AND n.partition_date = r.partition_date
    LEFT JOIN push_sent AS s
        ON
            r.user_id = s.user_id
            AND r.request_id = s.request_id
            AND r.push_type = s.push_type
            AND r.partition_date = s.partition_date
    GROUP BY 1, 2
)

SELECT
    s.partition_date,
    s.push_type,
    s.notification_requests,
    COALESCE(r.requests_cnt_ctr_v9_extended, 0) AS requests_cnt_ctr_v9_extended,
    COALESCE(r.requests_cnt_q_touch_v1, 0) AS requests_cnt_q_touch_v1,
    COALESCE(r.ranking_cnt_ctr_v9_extended, 0) AS ranking_cnt_ctr_v9_extended,
    COALESCE(r.ranking_cnt_q_touch_v1, 0) AS ranking_cnt_q_touch_v1,
    COALESCE(r.avg_requests_per_user, 0) AS avg_requests_per_user,
    COALESCE(r.avg_weight_ctr_v9_extended, 0) AS avg_weight_ctr_v9_extended,
    COALESCE(r.avg_weight_q_touch_v1, 0) AS avg_weight_q_touch_v1,
    COALESCE(sr.sents, 0) AS sents,
    COALESCE(f.request_to_ranked, 0) AS request_to_ranked,
    COALESCE(f.ranked_to_sent, 0) AS ranked_to_sent,
    COALESCE(f.request_to_sent, 0) AS request_to_sent,
    COALESCE(f.ranked_to_sent_v9_extended, 0) AS ranked_to_sent_v9_extended,
    COALESCE(f.ranked_to_sent_q_touch_v1, 0) AS ranked_to_sent_q_touch_v1
FROM stats_notification_request AS s
LEFT JOIN stat_ranking AS r
    ON
        s.push_type = r.push_type
        AND s.partition_date = r.partition_date
LEFT JOIN sent_ranking AS sr
    ON
        r.push_type = sr.push_type
        AND r.partition_date = sr.partition_date
LEFT JOIN funnel AS f
    ON
        s.push_type = f.push_type
        AND s.partition_date = f.partition_date
