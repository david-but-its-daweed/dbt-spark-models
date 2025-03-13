{{
  config(
    materialized='incremental',
    alias='active_devices',
    file_format='parquet',
    incremental_strategy='insert_overwrite',
    partition_by=['month_msk'],
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
        dvc.device_id,
        dvc.date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(TO_DATE(dvc.join_ts_msk)) AS join_dt,
        FIRST_VALUE(UPPER(dvc.country)) AS country,
        FIRST_VALUE(LOWER(dvc.os_type)) AS platform,
        FIRST_VALUE(dvc.os_version) AS os_version,
        FIRST_VALUE(dvc.app_version) AS app_version,
        MIN(dvc.ephemeral) AS is_ephemeral,
        FIRST_VALUE(dvc.real_user_id) AS real_user_id,
        FIRST_VALUE(IF(dvc.legal_entity = 'jmt', 'JMT', 'SIA')) AS legal_entity,
        FIRST_VALUE(CASE WHEN dvc.date_msk < '2023-07-01' THEN 'Joom' ELSE ent.app_entity_gold END) AS app_entity,
        FIRST_VALUE(dvc.custom_domain) as custom_domain,
        FIRST_VALUE(UPPER(dvc.language)) AS app_language
    FROM {{ source('mart', 'star_active_device') }} AS dvc
    LEFT JOIN {{ ref('app_entities_mapping') }} AS ent USING (app_entity)
    {% if is_incremental() %}
        WHERE dvc.date_msk >= TRUNC(DATE '{{ var("start_date_ymd") }}' - INTERVAL 200 DAYS, 'MM')
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
    CASE
        WHEN UPPER(d.app_entity) = "SHOPY" THEN d.custom_domain
        ELSE Null
    END AS shopy_blogger_domain,
    d.is_ephemeral,
    d.day = join_day AS is_new_user,
    d.real_user_id,
    d.legal_entity,
    d.app_language,
    TRUNC(d.day, 'MM') AS month_msk
FROM device_info AS d
INNER JOIN min_dates USING (device_id)
DISTRIBUTE BY month_msk, ABS(HASH(d.device_id)) % 10
