{{
  config(
    materialized='table',
    alias='active_users',
    file_format='delta',
  )
}}

SELECT
    *,
    MAX(day = join_day) OVER (PARTITION BY user_id, day) AS is_new_user
FROM (
    SELECT
        user_id,
        date_msk AS day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards 
        MIN(date_msk) OVER (PARTITION BY user_id) AS join_day,
        FIRST_VALUE(UPPER(country)) AS country,
        FIRST_VALUE(LOWER(os_type)) AS platform,
        FIRST_VALUE(os_version) AS os_version,
        FIRST_VALUE(app_version) AS app_version,
        MIN(ephemeral) AS is_ephemeral
    FROM {{ source('mart', 'star_active_device') }}
    WHERE
        TRUE
        {% if is_incremental() %}
            AND DATEDIFF(TO_DATE('{{ var("start_date_ymd") }}'), date_msk) < 181
        {% elif target.name != 'prod' %}
            AND DATEDIFF(TO_DATE('{{ var("start_date_ymd") }}'), date_msk) < 181
        {% endif %}

    GROUP BY 1, 2
)