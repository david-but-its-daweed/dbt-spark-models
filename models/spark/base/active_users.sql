{{
  config(
    materialized='incremental',
    alias='active_users',
    file_format='parquet',
    incremental_strategy='insert_overwrite',
    partition_by=['month'],
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'day',
        'bigquery_upload_horizon_days': '230',
        'priority_weight': '1000',
    },
  )
}}


WITH user_info AS (
    SELECT
        user_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral,
        FIRST_VALUE(real_user_id) AS real_user_id,
        FIRST_VALUE(IF(legal_entity = 'jmt', 'JMT', 'Joom')) AS legal_entity,
        FIRST_VALUE(UPPER(language)) AS app_language
    FROM {{ source('mart', 'star_active_device') }}
    {% if is_incremental() %}
       WHERE date_msk >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
    {% endif %}
    GROUP BY 1, 2
),

join_dates AS (
    SELECT
        user_id,
        MIN(date_msk) AS join_day
    FROM {{ source('mart', 'star_active_device') }}
    GROUP BY 1
)

SELECT
    u.user_id,
    u.day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
    jd.join_day,
    u.country,
    u.platform,
    u.os_version,
    u.app_version,
    u.is_ephemeral,
    u.day = jd.join_day AS is_new_user,
    u.real_user_id,
    u.legal_entity,
    u.app_language,
    TRUNC(u.day, 'MM') AS month
FROM user_info AS u
INNER JOIN join_dates AS jd USING (user_id)
