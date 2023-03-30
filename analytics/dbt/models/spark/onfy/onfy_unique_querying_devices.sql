{{ config(
    schema='onfy',
    materialized='incremental',
    file_format='delta',
    meta = {
      'team': 'onfy'
    }
) }}
WITH human_devices AS (
  SELECT device_id
  FROM {{ source('onfy_mart', 'auth_proxy_devices_without_bots') }}
  WHERE is_bot IS FALSE
), 
onfy_unique_querying_devices AS (
  SELECT 
    '5m' AS frequency,
    TIMESTAMP(FROM_UNIXTIME(CAST(UNIX_TIMESTAMP(published_at) / 300 AS INT) * 300)) AS ts,
    bf.device_id,
    COALESCE(isp, 'unknown') AS isp,
    COALESCE(user_agent, 'unknown') AS user_agent
  FROM {{ source('threat', 'bot_factors_onfy') }} bf
  JOIN human_devices USING (device_id)
  WHERE
    published_at >= DATE'{{ var("start_date_ymd") }}'
    AND published_at < DATE'{{ var("end_date_ymd") }}'
)
SELECT 
  frequency, 
  ts, 
  isp, 
  user_agent, 
  APPROX_COUNT_DISTINCT(device_id) AS unique_querying_devices
FROM onfy_unique_querying_devices
GROUP BY CUBE(frequency, ts, isp, user_agent)
HAVING frequency IS NOT NULL AND ts IS NOT NULL
