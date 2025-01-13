{{ config(
    schema='customer_routing',
    incremental_strategy='insert_overwrite',
    materialized='incremental',
    file_format='parquet',
    partition_by=['partition_date_msk'],
    meta = {
      'team': 'clan',
      'model_owner': '@product-analytics.duty',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}

SELECT DISTINCT
    device_id,
    partition_date AS partition_date_msk,
    DATE(FROM_UNIXTIME(event_ts / 1000)) AS open_date_msk,
    device.os_type,
    FIRST(payload.avail) OVER (PARTITION BY device_id, DATE(FROM_UNIXTIME(event_ts / 1000)) ORDER BY FROM_UNIXTIME(event_ts / 1000)) AS avail_flg,
    FIRST(payload.productid) OVER (PARTITION BY device_id, DATE(FROM_UNIXTIME(event_ts / 1000)) ORDER BY FROM_UNIXTIME(event_ts / 1000)) AS product_id
FROM {{ source('mart', 'device_events') }}
WHERE
    type IN ('productOpenServer')
    {% if is_incremental() %}
        AND partition_date >= DATE'{{ var("start_date_ymd") }}'
        AND partition_date < DATE'{{ var("end_date_ymd") }}'
    {% else %}
        AND partition_date >= DATE'2022-06-01'
    {% endif %}

