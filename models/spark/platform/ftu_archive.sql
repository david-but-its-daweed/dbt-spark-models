{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='incremental',
    partition_by=['partition_date'],
    incremental_strategy='insert_overwrite',
    file_format='parquet'
) }}

SELECT
    platform,
    table_name,
    TO_DATE(CASE
        WHEN HOUR(start_time) >= 22 THEN DATE_TRUNC('Day', start_time + INTERVAL 24 HOURS)
        ELSE DATE_TRUNC('Day', start_time)
    END) AS partition_date,
    -- we don't use next_start_time in mongo table because they run in parallel and next_start_time is incorrect
    MIN(
        CASE
            WHEN table_name LIKE 'mongo%' THEN start_time
            WHEN table_name IN ('mart.fact_install') THEN start_time
            ELSE COALESCE(next_start_time, start_time)
        END
    ) AS start_date,
    MIN(dttm) AS end_date,
    UNIX_TIMESTAMP(MIN(dttm)) - UNIX_TIMESTAMP(MIN(IF(table_name LIKE 'mongo%', start_time, COALESCE(next_start_time, start_time)))) AS duration
FROM platform.fact_table_update
WHERE
    start_time IS NOT NULL
    {% if is_incremental() %}
        AND DATE(start_time) >= DATE '{{ var("start_date_ymd") }}'
        AND DATE(start_time) < DATE '{{ var("end_date_ymd") }}'
    {% else %}
        AND DATE(start_time) > NOW() - INTERVAL 6 MONTH
    {% endif %}
GROUP BY
    platform,
    table_name,
    TO_DATE(CASE
        WHEN HOUR(start_time) >= 22 THEN DATE_TRUNC('Day', start_time + INTERVAL 24 HOURS)
        ELSE DATE_TRUNC('Day', start_time)
    END)
