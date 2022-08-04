{{ config(
    schema='example',
    materialized='delta',
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
    country IN ('RU', 'DE', 'FR', 'GB', 'IT', 'ES', 'CH', 'SE', 'IL', 'UA', 'MD', 'BY')
    AND platform in ('android', 'ios')
GROUP BY
    t, country, platform
