{{
  config(
    materialized='incremental',
    alias='active_devices',
    file_format='parquet',
    incremental_strategy='insert_overwrite',
    partition_by=['month'],
    meta = {
        'model_owner' : '@general_analytics',
        'bigquery_load': 'true',
        'bigquery_partitioning_date_column': 'day',
        'bigquery_upload_horizon_days': '230',
        'priority_weight': '1000',
        'full_reload_on': '6',
    }
  )
}}


WITH device_info AS (
    SELECT
        device_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(TO_DATE(join_ts_msk)) AS join_dt,
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral,
        FIRST_VALUE(real_user_id) AS real_user_id,
        FIRST_VALUE(IF(legal_entity = 'jmt', 'JMT', 'Joom')) AS legal_entity,
        FIRST_VALUE(
            CASE
                WHEN date_msk < '2023-07-01' OR app_entity = 'joom'
                    THEN 'Joom'
                WHEN app_entity = 'joom_geek'
                    THEN 'Joom Geek'
                WHEN app_entity = 'cool_be'
                    THEN 'CoolBe'
                WHEN app_entity = 'cool_be_com'
                    THEN 'CoolBeCom'
                WHEN app_entity = 'cool_be_trending'
                    THEN 'CoolBe Trending'
            END
        ) AS app_entity,
        FIRST_VALUE(UPPER(language)) AS app_language
    FROM {{ source('mart', 'star_active_device') }}
    {% if is_incremental() %}
        WHERE date_msk >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
    {% endif %}
    GROUP BY 1, 2
),

min_dates AS (
    SELECT
        device_id,
        MIN(date_msk) AS dt
    FROM {{ source('mart', 'star_active_device') }}
    GROUP BY 1
)

SELECT
    d.device_id,
    d.day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
    IF(
        min_dates.dt < d.join_dt,
        min_dates.dt,
        d.join_dt
    ) AS join_day,
    d.country,
    d.platform,
    d.os_version,
    d.app_version,
    d.app_entity,
    d.is_ephemeral,
    d.day = join_day AS is_new_user,
    d.real_user_id,
    d.legal_entity,
    d.app_language,
    TRUNC(d.day, 'MM') AS month
FROM device_info AS d
INNER JOIN min_dates USING (device_id)
CLUSTER BY month, abs(hash(d.device_id)) % 10