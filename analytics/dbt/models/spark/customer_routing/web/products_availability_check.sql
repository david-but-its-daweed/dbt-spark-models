{{ config(
    schema='customer_routing',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'clan',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

SELECT DISTINCT
    device_id,
    partition_date AS partition_date_msk,
    DATE(event_ts_msk) AS day_msk,
    device.os_type,
    FIRST(payload.avail) OVER (PARTITION BY c.device_id, DATE(event_ts_msk) ORDER BY event_ts_msk) AS avail_flg,
    FIRST(payload.productId) OVER (PARTITION BY c.device_id, DATE(event_ts_msk) ORDER BY event_ts_msk) AS product_id
FROM {{ source('mart', 'device_events') }}
WHERE
    `type` IN ('productOpenServer')
    AND device.os_type LIKE "%web%"
    {% if is_incremental() %}
        AND partition_date >= date'{{ var("start_date_ymd") }}'
        AND partition_date < date'{{ var("end_date_ymd") }}'
    {% else %}
        AND partition_date >= date'2022-06-01'
    {% endif %}

