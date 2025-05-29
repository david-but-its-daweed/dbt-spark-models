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
        {% if is_incremental() %}
            AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-05-20")
        {% endif %}
),

sent AS (
    SELECT
        partition_date,
        user_id,
        email_type,
        language,
        email_id,
        col.id AS text_id,
        col.place AS text_place,
        col.text
    FROM sentd_stg
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
            AND partition_date >= DATE'{{ var("start_date_ymd") }}' - INTERVAL 1 DAY
        {% else %}
            AND partition_date >= DATE("2025-05-20")
        {% endif %}
)

SELECT
    s.partition_date,
    s.email_type,
    s.text_id,
    s.text_place,
    s.text,
    s.language,
    COUNT(DISTINCT s.email_id) AS email_sents,
    COUNT(DISTINCT l.utm_email_id) AS email_click
FROM sent AS s
LEFT JOIN link AS l ON s.email_id = l.utm_email_id
WHERE
    s.text_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5, 6


