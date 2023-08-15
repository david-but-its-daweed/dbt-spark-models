{{
  config(
    materialized='table',
    partition_by={
      "field": "day",
    }
  )
}}

SELECT
    *,
    MAX(day = join_day) OVER (PARTITION BY device_id, day) AS is_new_user
FROM (
    SELECT
        device_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        IF(
            MIN(date_msk) OVER (PARTITION BY device_id) < FIRST_VALUE(TO_DATE(join_ts_msk)),
            MIN(date_msk) OVER (PARTITION BY device_id),
            FIRST_VALUE(TO_DATE(join_ts_msk))
        ) AS join_day,
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral
    FROM {{ source('mart', 'star_active_device') }}
    GROUP BY 1, 2
)
