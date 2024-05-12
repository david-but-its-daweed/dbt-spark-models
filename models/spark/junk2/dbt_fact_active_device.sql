{{ config(
    meta = {
      'model_owner' : '@aplotnikov'
    },
    schema='junk2',
    materialized='table',
    partition_by=['partition_date_msk'],
    file_format='parquet'
) }}
-- comment
select
  partition_date as partition_date_msk,
  device_id
from {{ source('mart', 'device_events') }}
where
  `type` in ('sessionStart', 'sessionConfigured')
  and partition_date >= date'{{ var("start_date_ymd") }}'
  and partition_date < date'{{ var("end_date_ymd") }}'
group by partition_date, device_id