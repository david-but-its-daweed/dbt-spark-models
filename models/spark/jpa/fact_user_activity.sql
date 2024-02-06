{{ config(
    schema='joompro_analytics',
    partition_by=['partition_date'],
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    meta = {
      'model_owner': '@aplotnikov',
      'team': 'general_analytics',
    }
) }}

SELECT
    DISTINCT
    partition_date,
    request_id,
    published_at,
    bf.device_id,
    auth_user_id as user_id,
    country,
    city,
    user_agent,
    request_path
FROM {{ source('threat', 'bot_factors_joompro') }} bf
INNER JOIN (
    SELECT
        DISTINCT
        auth_user_id,
        device_id
    FROM {{ source('joompro_mart', 'auth_proxy_dim_devices') }}
    WHERE
        auth_user_id IS NOT NULL
) ap ON bf.device_id = ap.device_id
WHERE
    request_path RLIKE '/dashboard'
    AND response_code >= 200
    AND response_code <= 299
    AND user_agent NOT IN (
        'GoogleStackdriverMonitoring-UptimeChecks(https://cloud.google.com/monitoring)'
    )
    {% if is_incremental() %}
    AND partition_date >= date'{{ var("start_date_ymd") }}'
    AND partition_date < date'{{ var("end_date_ymd") }}'
    {% else %}
    and partition_date >= date'2023-09-20'
    {% endif %}

     

