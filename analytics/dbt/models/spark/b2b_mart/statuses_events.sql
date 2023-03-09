{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

with products_statuses as (
select merchant_order_id, product_id, status, event_ts_msk
    from
    (select 
        payload.merchantOrderId as merchant_order_id,
        payload.productId as product_id,
        payload.psiStatusId as psi_status_id,
        payload.status,
        from_unixtime(payload.updatedTime/1000) as event_ts_msk,
        row_number() over (partition by payload.merchantOrderId, payload.productId, payload.status order by event_ts_utc desc) as rn
    from {{ source('b2b_mart', 'operational_events') }}
    where type = 'orderProductStatusChanged'
    )
    where rn = 1
),

merchant_order as (
select distinct merchant_order_id, order_id, merchant_id from {{ ref('fact_merchant_order') }}
where next_effective_ts_msk is null),

merchant_order_statuses as (
select
merchant_order_id, merchant_id, order_id, status, event_ts_msk, moderator_id
from
(
select 
payload.id as merchant_order_id,
payload.merchantId as merchant_id,
payload.moderatorId as moderator_id,
payload.orderId as order_id,
payload.status,
from_unixtime(payload.updatedTime/1000) as event_ts_msk,
row_number() over (partition by payload.id, payload.status order by event_ts_utc desc) as rn
from {{ source('b2b_mart', 'operational_events') }}
    where type = 'merchantOrderChanged'
)
where rn = 1
),

broker_order as (
select
broker_id, moderator_id, order_id, status, event_ts_msk
from
(
select 
payload.brokerId as broker_id,
payload.moderatorId as moderator_id,
payload.orderId as order_id,
payload.brokerOrderStatus as status,
from_unixtime(payload.updatedTime/1000) as event_ts_msk,
lag(payload.brokerOrderStatus) over (partition by payload.orderId order by event_ts_utc desc) as lag_status
from {{ source('b2b_mart', 'operational_events') }}
    where type = 'brokerOrderChanged'
)
where status != lag_status or lag_status is null
),

logistician_order as (
select
broker_id, moderator_id, order_id, status, event_ts_msk
from
(
select 
payload.brokerId as broker_id,
payload.moderatorId as moderator_id,
payload.orderId as order_id,
payload.logisticianOrderStatus as status,
from_unixtime(payload.updatedTime/1000) as event_ts_msk,
lag(payload.brokerOrderStatus) over (partition by payload.orderId order by event_ts_utc desc) as lag_status
from {{ source('b2b_mart', 'operational_events') }}
    where type = 'logisticianOrderChanged' or lag_status is null
)
where status != lag_status
)
,

order as (
select
order_id, status, sub_status, event_ts_msk
from
(
select 
payload.orderId as order_id,
payload.status as status,
payload.subStatus as sub_status,
from_unixtime(payload.updatedTime/1000) as event_ts_msk,
row_number() over (partition by payload.orderId, payload.status, payload.subStatus order by event_ts_utc desc) as rn
from {{ source('b2b_mart', 'operational_events') }}
    where type = 'orderChangedByAdmin'
)
where rn = 1
)

select 
    order_id,
    merchant_order_id,
    '' as product_id,
    merchant_id,
    moderator_id,
    '' as broker_id,
    status,
    '' as sub_status,
    event_ts_msk,
    'merchant order' as entity
    from 
    merchant_order_statuses
union all 
select 
    m.order_id,
    p.merchant_order_id,
    product_id,
    m.merchant_id,
    '' as moderator_id,
    '' as broker_id,
    status,
    '' as sub_status,
    event_ts_msk,
    'product' as entity
    from 
    products_statuses p left join merchant_order m on p.merchant_order_id = m.merchant_order_id
union all 
select 
    order_id,
    '' as merchant_order_id,
    '' as product_id,
    '' as merchant_id,
    moderator_id,
    broker_id,
    status,
    '' as sub_status,
    event_ts_msk,
    'broker' as entity
    from 
    broker_order
union all 
select 
    order_id,
    '' as merchant_order_id,
    '' as product_id,
    '' as merchant_id,
    moderator_id,
    broker_id,
    status,
    '' as sub_status,
    event_ts_msk,
    'logistician' as entity
    from 
    logistician_order
union all 
select 
    order_id,
    '' as merchant_order_id,
    '' as product_id,
    '' as merchant_id,
    '' as moderator_id,
    '' as broker_id,
    status,
    sub_status,
    event_ts_msk,
    'order' as entity
    from 
    order
