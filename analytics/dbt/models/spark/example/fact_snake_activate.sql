{{ config(
    schema='example',
    materialized='incremental',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'bigquery_partitioning_date_column': 'partition_date_msk',
      'bigquery_fail_on_missing_partitions': 'false'
    }
) }}
select
  partition_date as partition_date_msk,
  device_id,
  event_ts
from {{ source('mart', 'device_events') }}
where
  `type` in ('snakeActivate')
{% if is_incremental() %}
  and partition_date >= date'{{ var("start_date_ymd") }}'
  and partition_date < date'{{ var("end_date_ymd") }}'
{% else %}
  and partition_date >= date'2022-12-01'
{% endif %}
