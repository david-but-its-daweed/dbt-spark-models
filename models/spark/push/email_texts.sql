{{
  config(
    incremental_strategy='insert_overwrite',
    materialized = 'incremental',
    file_format = 'parquet',
    partition_by = ['partition_date'],
    alias = 'email_texts',
    schema = 'push',
    meta = {
        'model_owner' : '@ekutynina',
        'bigquery_load': 'true'
    }
  )
}}


WITH sentd_stg AS (
    SELECT
        partition_date,
        user_id,
        payload.type AS email_type,
        device.language,
        payload.emailId AS email_id,
        EXPLODE(payload.emailTexts) AS col

    FROM {{ source('mart', 'device_events') }}
    WHERE
        type = "emailRequestSent"
        AND ephemeral = FALSE
        AND payload.emailTexts IS NOT NULL
        {% if is_incremental() %}
            AND partition_date >= DATE '{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-05-20")
        {% endif %}
),

text AS (
    SELECT
        key,
        val AS text,
        SPLIT(key, "\\.")[SIZE(SPLIT(key, "\\.")) - 1] AS text_id
    FROM {{ source('mongo', 'core_i18ndata_daily_snapshot') }}
    WHERE
        key LIKE "%mail%"
        AND langCode = "ru"
        AND REGEXP_LIKE(SPLIT(key, "\\.")[SIZE(SPLIT(key, "\\.")) - 1], "^[0-9]+$")
),

sent AS (
    SELECT
        s.partition_date,
        s.user_id,
        s.email_type,
        s.language,
        s.email_id,
        s.col.place AS text_place,
        s.col.id AS text_id,
        t.text
    FROM sentd_stg AS s
    LEFT JOIN text AS t ON s.col.id = t.text_id
),

link AS (
    SELECT
        user_id,
        payload.params.utm_email_id
    FROM {{ source('mart', 'device_events') }}
    WHERE
        type = "externalLink"
        AND payload.params.utm_email_id IS NOT NULL
        {% if is_incremental() %}
            AND partition_date >= DATE '{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-05-20")
        {% endif %}
),

opens AS (
    SELECT
        user_id,
        email_id
    FROM {{ source('push', 'mail_user_events') }}
    WHERE
        event_type = "emailOpen"
        {% if is_incremental() %}
            AND partition_date >= DATE '{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-05-20")
        {% endif %}
)

SELECT
    partition_date,
    email_type,
    language,
    preview_text_id,
    subject_text_id,
    preview_text,
    subject_text,
    COUNT(email_id) AS emails,
    SUM(clicks) AS clicks,
    SUM(opens) AS opens,
    FIRST(texts) AS full_text
FROM (
    SELECT
        s.partition_date,
        s.email_type,
        s.language,
        s.email_id,
        COUNT(DISTINCT o.email_id) AS opens,
        COUNT(DISTINCT l.utm_email_id) AS clicks,
        CAST(ROUND(MAX(IF(s.text_place = "preview", s.text_id, NULL)), 0) AS STRING) AS preview_text_id,
        CAST(ROUND(MAX(IF(s.text_place = "subject", s.text_id, NULL)), 0) AS STRING) AS subject_text_id,
        MAX(IF(s.text_place = "preview", s.text, NULL)) AS preview_text,
        MAX(IF(s.text_place = "subject", s.text, NULL)) AS subject_text,
        CONCAT_WS(",", COLLECT_LIST(s.text_place || s.text_id || " ," || s.text)) AS texts
    FROM sent AS s
    LEFT JOIN link AS l ON s.email_id = l.utm_email_id
    LEFT JOIN opens AS o ON s.email_id = o.email_id
    GROUP BY 1, 2, 3, 4
)
GROUP BY 1, 2, 3, 4, 5, 6, 7

