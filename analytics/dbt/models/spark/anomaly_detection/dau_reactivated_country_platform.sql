{{ config(
    schema='anomaly_detection',
    materialized='table',
    file_format='delta',
    meta = {
      'predictor_enabled': 'true',
      'anomalies_channel': '#aplotnikov-notifications',
      'anomalies_metric_name': 'DAU reactivated',
    }
) }}
WITH active_devices AS (
    SELECT DISTINCT device_id,
        date_msk AS date,
        DATE(join_ts_msk) AS join_date,
        UPPER(country) AS country,
        LOWER(os_type) AS platform
    FROM {{ source('mart', 'star_active_device') }}
    WHERE ephemeral = FALSE),

     active_devices_2 AS (
    SELECT device_id,
        date,
        LAG(date) OVER (PARTITION BY device_id ORDER BY date) AS prev_date,
        join_date,
        country,
        platform
    FROM active_devices), 

     active_devices_3 AS (
    SELECT device_id,
        date, 
        COALESCE(DATEDIFF(date, prev_date), 0) as active_lag,
        join_date,
        country,
        platform
    FROM active_devices_2),

     active_devices_4 AS (
    SELECT device_id,
        date,
        country,
        platform,
        IF(date = join_date, 1, 0) AS is_new,
        IF(date != join_date AND active_lag < 28, 1, 0) AS is_regular,
        IF(date != join_date AND active_lag > 28, 1, 0) AS is_reactivated
    FROM active_devices_3)

SELECT date as t,
    country,
    platform,
    SUM(is_reactivated) AS y
FROM active_devices_4
WHERE WHERE country IN ('RU', 'DE', 'FR', 'GB', 'IT', 'ES', 'CH', 'SE', 'IL', 'UA', 'MD', 'BY')
    AND platform in ('android', 'ios')
GROUP BY t, country, platform