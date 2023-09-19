{{
  config(
    meta = {
      'model_owner' : '@gusev'
    },
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['day', 'device_id'],
    partition_by=['month'],
    alias='active_devices',
    file_format='delta',
    incremental_predicates=["DBT_INTERNAL_DEST.month >= trunc(current_date() - interval 300 days, 'MM')"]
  )
}}

/*
    Зависимости (todo: проверить. за какой период обновляются , подобрать период для инкрементального расчета)
     - fact_active_device
     - dim_device_min
     - link_device_ad_partner
     - link_device_real_user
 */
with device_info as (
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
        where date_msk >= date'{{ var("start_date_ymd") }}' - interval 270 days
    {% endif %}
    group by 1, 2
),

min_dates as (
    select device_id, min(date_msk) as dt
    from  {{ source('mart', 'star_active_device') }}
    group by 1
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
    d.is_ephemeral,
    d.day = join_day as is_new_user,
    trunc(d.day, 'MM') as month
FROM device_info d
join min_dates using (device_id)
