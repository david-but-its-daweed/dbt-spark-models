{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['day', 'device_id'],
    partition_by=['day'],
    incremental_predicates=[
        "datediff(TO_DATE('{{ var(\"start_date_ymd\") }}'), TO_DATE(DBT_INTERNAL_DEST.day)) < 181",
        "datediff(TO_DATE('{{ var(\"start_date_ymd\") }}'), TO_DATE(DBT_INTERNAL_DEST.day)) >= 0",
    ],
    alias='active_devices',
    file_format='delta',
  )
}}


with join_days as (
  SELECT
    device_id,
     IF(
            MIN(date_msk) < FIRST_VALUE(TO_DATE(join_ts_msk)),
            MIN(date_msk),
            FIRST_VALUE(TO_DATE(join_ts_msk))
        ) AS join_day
  FROM {{ source('mart', 'star_active_device') }}
  group by 1
)

SELECT
    *,
    MAX(day = join_day) OVER (PARTITION BY device_id, day) AS is_new_user
FROM (
    SELECT
        a.device_id,
        a.date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards
        FIRST_VALUE(b.join_day) as join_day,
        FIRST_VALUE(UPPER(a.country)) AS country,
        FIRST_VALUE(LOWER(a.os_type)) AS platform,
        FIRST_VALUE(a.os_version) AS os_version,
        FIRST_VALUE(a.app_version) AS app_version,
        MIN(a.ephemeral) AS is_ephemeral
    FROM {{ source('mart', 'star_active_device') }} a
    left join join_days b using(device_id)
    WHERE
        TRUE
        {% if is_incremental() or target.name != 'prod' %}
            AND DATEDIFF(TO_DATE('{{ var("start_date_ymd") }}'), date_msk) < 181
            AND DATEDIFF(TO_DATE('{{ var("start_date_ymd") }}'), date_msk) >= 0
        {% endif %}

    GROUP BY 1, 2
)
