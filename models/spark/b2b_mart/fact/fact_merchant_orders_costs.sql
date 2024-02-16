{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


select
merchant_order_id,
advance,
advance_currency,
remain,
remain_currency,
total,
total_currency,
event_ts_msk as effective_ts
from
(
 select 
     payload.id as merchant_order_id,
     payload.advance.amount as advance,
     payload.advance.ccy as advance_currency,
     payload.remain.amount as remain,
     payload.remain.ccy as remain_currency,
     payload.total.amount as total,
     payload.total.ccy as total_currency,
     event_ts_msk,
     partition_date,
     row_number() over (partition by id order by event_ts_msk desc) as rn
 from {{ source('b2b_mart', 'operational_events') }}
 where type = 'merchantOrderPaymentPriceUpdated'
 )
 where rn = 1
