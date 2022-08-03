{{ config(
    schema='example',
    materialized='view',
    meta = {
      'predictor_enabled': 'true'
    }
) }}
WITH active_devices AS (
SELECT
    DISTINCT device_id,
    date_msk AS t,
    UPPER(country) AS country,
    LOWER(os_type) AS platform
FROM mart.star_active_device
WHERE
    ephemeral = FALSE
)
SELECT
    t,
    country,
    platform,
    COUNT(device_id) AS y
FROM active_devices
WHERE
    country IS NOT NULL
    AND platform IS NOT NULL
GROUP BY
    t, country, platform
