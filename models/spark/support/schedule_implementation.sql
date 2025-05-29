{{
  config(
    meta = {
      'model_owner' : '@operational.analytics.duty',
      'bigquery_load': 'true',
      'bigquery_overwrite': 'true',      
    },
    schema='support',
    file_format='delta',
    materialized='incremental',
    incremental_strategy='insert_overwrite'
  )
}}

WITH agents_data AS (
    SELECT
        sb_user_id,
        MAX_BY(function, partition_date) AS function,
        MAX_BY(holistics_team, partition_date) AS holistics_team,
        MAX_BY(email, partition_date) AS email
    FROM {{ source('support_mart', 'agents') }}
    GROUP BY 1
),

collect_data_from_roasters AS (
    SELECT
        DATE(date) AS shift_date,
        user_id,
        TO_TIMESTAMP(date || " " || starttime, "yyyy-MM-dd HH:mm:ss") AS start_datetime,
        TO_TIMESTAMP(date || " " || endtime, "yyyy-MM-dd HH:mm:ss")
        + IF(endtime < starttime, INTERVAL 1 DAY, INTERVAL 0 DAY) AS end_datetime,
        IF(detailed = "", "unknown", COALESCE(detailed, "unknown")) AS detailed
    FROM {{ source('support', 'shiftbase_rosters_latest_snapshot') }}
    WHERE date BETWEEN DATE_SUB(CURRENT_DATE, 30) AND DATE_ADD(CURRENT_DATE, 30)
),

intervals AS (
    SELECT
        *,
        EXPLODE(
            SEQUENCE(
            -- Округляем вниз до начала часа
                DATE_TRUNC("HOUR", start_datetime),
                end_datetime,
                INTERVAL 1 HOUR
            )
        ) AS working_hour_start
    FROM collect_data_from_roasters
),

with_overlap AS (
    SELECT
        *,
        GREATEST(start_datetime, working_hour_start) AS overlap_start,
        -- working_hour_start + INTERVAL 1 HOUR - working_hour_end не собираем отдельное поле, тк нужно только тут
        LEAST(end_datetime, working_hour_start + INTERVAL 1 HOUR) AS overlap_end
    FROM intervals
),

final AS (
    SELECT
        *,
        -- Разница в секундах → часы
        ROUND(
            (UNIX_TIMESTAMP(overlap_end) - UNIX_TIMESTAMP(overlap_start)) * 1.0 / 3600.0,
            2
        ) AS working_hours
    FROM with_overlap
)

SELECT
    t2.*,
    t1.shift_date,
    t1.hour_start,
    t1.detailed,
    t1.working_hours,
    CASE
        WHEN t2.holistics_team IN ("JoomPro Merchants", "JoomPro support") THEN "JoomPro"
        WHEN t2.holistics_team = "Onfy" THEN "Onfy"
        ELSE "Marketplace"
    END AS business_unit
FROM final AS t1
LEFT JOIN agents_data AS t2
    ON t1.user_id = t2.sb_user_id
WHERE t2.`function` NOT IN ("Moderator", "Merchant Support")