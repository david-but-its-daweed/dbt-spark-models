{{ config(
    schema='b2b_mart',
    materialized='view',
    partition_by={
         "field": "event_msk_date"
    },
    meta = {
      'model_owner' : '@kirill_melnikov',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}



select user['userId'] AS user_id,
event_ts_msk,
cast(event_ts_msk as DATE) as event_msk_date,
type,
payload.pageName as page_name,
payload.pageUrl as page_url
from {{ source('b2b_mart', 'device_events') }} main
        where type = 'pageView'
