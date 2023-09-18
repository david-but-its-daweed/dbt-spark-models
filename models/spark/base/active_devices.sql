{{
  config(
    meta = {
      'model_owner' : '@gusev'
    },
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['day', 'device_id']
    alias='active_devices',
    file_format='delta',
  )
}}

/*
    Зависимости (todo: проверить. за какой период обновляются , подобрать период для инкрементального расчета)
     - fact_active_device
     - dim_device_min
     - link_device_ad_partner
     - link_device_real_user
 */
with min_dates as (
    select device_id, min(date_msk) as dt
    from  {{ source('mart', 'star_active_device') }}
    group by 1
),
device_stats as (
    select
        device_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(TO_DATE(join_ts_msk)) as join_dt,
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral
    FROM {{ source('mart', 'star_active_device') }}
    {% if is_incremental() %}
        where date_msk >= date'{{ var("start_date_ymd") }}' - interval 190 days
    {% endif %}
    group by 1, 2
)
SELECT
    device_stats.device_id,
    device_stats.day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
    IF(
        min_dates.dt < device_stats.join_dt,
        min_dates.dt,
        device_stats.join_dt
    ) AS join_day,
    device_stats.country,
    device_stats.platform,
    device_stats.os_version,
    device_stats.app_version,
    device_stats.is_ephemeral,
    device_stats.day = join_day as is_new_user
FROM device_stats
join min_dates using (device_id)
