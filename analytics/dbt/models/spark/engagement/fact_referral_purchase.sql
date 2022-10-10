{{ config(
    schema='engagement',
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
select
  partition_date as partition_date_msk,
  device_id,
  user_id,
  timestamp(millis_to_ts(event_ts)) as event_ts,
  timestamp(millis_to_ts_msk(event_ts)) as event_ts_msk,
  payload.referrerId as referrer_id,
  payload.effectiveUSD as effective_usd,
  payload.orderId as order_id,
  payload.revenueShareType as revenueShare_type,
  payload.productCollectionId as productCollection_id,
  payload.socialPostId as socialPost_id
from {{ source('mart', 'device_events') }}
where
  `type` in ('referralPurchase')
{% if is_incremental() %}
  and partition_date >= date'{{ var("start_date_ymd") }}'
  and partition_date < date'{{ var("end_date_ymd") }}'
{% else %}
  and partition_date >= date'2022-06-01'
{% endif %}
