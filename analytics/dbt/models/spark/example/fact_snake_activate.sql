{{ config(
    schema='example',
    materialized='table',
    partition_by=['partition_date_msk'],
    file_format='delta',
    meta = {
      'team': 'general_analytics'
    }
) }}
select
  partition_date as partition_date_msk,
  device_id,
  event_ts
from {{ source('mart', 'device_events') }}
where
  `type` in ('snakeActivate')
  and partition_date >= date'2022-06-01'
