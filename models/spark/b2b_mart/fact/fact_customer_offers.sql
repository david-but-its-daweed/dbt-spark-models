{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


select 
_id as offer_id,
brokerTableLink as broker_table_link,
comment,
csmrReqId as customer_request_id,
dealId as deal_id,
merchantId as merchant_id,
ot.type as offer_type,
orderId as order_id,
ko.status,
userId as user_id,
brokerProps.link as broker_link,
millis_to_ts_msk(ctms) as created_time,
millis_to_ts_msk(utms) as updated_time
from {{ ref('scd2_customer_offers_snapshot') }} co
left join {{ ref('key_offer_status') }} ko on co.status = cast(ko.id as int)
left join {{ ref('key_offer_type') }} ot on co.offerType = cast(ot.id as int)
where dbt_valid_to is null
