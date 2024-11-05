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
    MIN(start_time) AS start_date,
    MIN(dttm) AS end_date,
    UNIX_TIMESTAMP(MIN(dttm)) - UNIX_TIMESTAMP(MIN(start_time)) AS duration
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
