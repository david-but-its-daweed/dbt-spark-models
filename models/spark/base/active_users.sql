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
    MAX(day = join_day) OVER(PARTITION BY user_id, day) AS is_new_user
FROM (
    SELECT  
        user_id,
        date_msk as day,  -- please, do not add any other columns to group by (e.g. user_id), it will influence DAU dashboards 
        MIN(date_msk) OVER(PARTITION BY user_id) as join_day,
        FIRST_VALUE(UPPER(country)) as country,
        FIRST_VALUE(LOWER(os_type)) as platform,
        FIRST_VALUE(os_version) as os_version,
        FIRST_VALUE(app_version) as app_version,
        MIN(ephemeral) as is_ephemeral
    FROM {{source('mart', 'star_active_device')}}
    GROUP BY 1,2
)