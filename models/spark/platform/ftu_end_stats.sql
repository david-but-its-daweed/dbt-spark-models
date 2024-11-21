{{ config(
    meta = {
      'model_owner' : '@analytics.duty'
    },
    schema='platform',
    materialized='table'
) }}

WITH ftu_data AS (
    SELECT
        table_name,
        platform AS table_type,
        partition_date,
        start_date,
        end_date,
        ROUND(duration / 60) AS duration
    FROM {{ ref("ftu_archive") }}
    WHERE partition_date > NOW() - INTERVAL 2 MONTH
),

ready_hours AS (
    SELECT
        ftu_data.table_name,
        ftu_data.table_type,
        ftu_data.duration,

        esd.effective_start_hours_msk,
        esd.effective_start_hours_cleared_msk,

        (UNIX_TIMESTAMP(ftu_data.start_date) - UNIX_TIMESTAMP(DATE(ftu_data.start_date))) / 60 / 60 AS start_time_hours,
        ((UNIX_TIMESTAMP(ftu_data.start_date) - UNIX_TIMESTAMP(DATE(ftu_data.start_date))) / 60 / 60 + 3) % 24 AS start_time_hours_msk,

        (UNIX_TIMESTAMP(ftu_data.end_date) - UNIX_TIMESTAMP(DATE(ftu_data.end_date))) / 60 / 60 AS ready_time_hours,
        ((UNIX_TIMESTAMP(ftu_data.end_date) - UNIX_TIMESTAMP(DATE(ftu_data.end_date))) / 60 / 60 + 3) % 24 AS ready_time_hours_msk
    FROM ftu_data
    LEFT JOIN {{ ref("effective_start_dates") }} AS esd
        USING (table_name, table_type, partition_date)
)

SELECT
    table_name,
    table_type,
    PERCENTILE_APPROX(duration, 0.5) AS median_duration,
    PERCENTILE_APPROX(start_time_hours, 0.5) AS median_start_time,
    PERCENTILE_APPROX(ready_time_hours, 0.5) AS median_end_time,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_msk, 0.5) AS p50_effective_duration,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_msk, 0.8) AS p80_effective_duration,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_cleared_msk, 0.5) AS p50_effective_duration_cleared,
    PERCENTILE_APPROX(ready_time_hours_msk - effective_start_hours_cleared_msk, 0.8) AS p80_effective_duration_cleared

FROM ready_hours
GROUP BY table_name, table_type