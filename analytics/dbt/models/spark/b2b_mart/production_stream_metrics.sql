{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

with statuses as (
  select payload.id as id,
    payload.status,
    min(TIMESTAMP(millis_to_ts_msk(payload.updatedTime))) as day
    from {{ source('b2b_mart', 'operational_events') }}
    WHERE `type`  ='merchantOrderChanged'
    group by payload.id,
        payload.status
    
),

merchant_orders as 
(select merchant_order_id, order_id, merchant_id, man_days,
    row_number() over (partition by order_id order by man_days desc) = 1 as longest_order,
    count(merchant_order_id) over (partition by order_id) = 1 as one_merchant_order,
    no_operations_started,
    advance_payment_requested,
    advance_payment_in_progress,
    advance_payment_acquired,
    manufacturing_and_qc_in_progress,
    remaining_payment_requested,
    remaining_payment_in_progress,
    remaining_payment_acquired,
    complete_payment_requested,
    complete_payment_in_progress,
    complete_payment_acquired,
    merchant_acquired_payment
    from
(
select 
    _id as merchant_order_id, 
    orderId as order_id,
    merchantId as merchant_id, 
    max(manDays) as man_days,
    min(case when payment_status = "noOperationsStarted" then day end) as no_operations_started,
    min(case when payment_status = "advancePaymentRequested" then day end) as advance_payment_requested,
    min(case when payment_status = "advancePaymentInProgress" then day end) as advance_payment_in_progress,
    min(case when payment_status = "advancePaymentAcquired" then day end) as advance_payment_acquired,
    min(case when payment_status = "manufacturingAndQcInProgress" then day end) as manufacturing_and_qc_in_progress,
    min(case when payment_status = "remainingPaymentRequested" then day end) as remaining_payment_requested,
    min(case when payment_status = "remainingPaymentInProgress" then day end) as remaining_payment_in_progress,
    min(case when payment_status = "remainingPaymentAcquired" then day end) as remaining_payment_acquired,
    min(case when payment_status = "completePaymentRequested" then day end) as complete_payment_requested,
    min(case when payment_status = "completePaymentInProgress" then day end) as complete_payment_in_progress,
    min(case when payment_status = "completePaymentAcquired" then day end) as complete_payment_acquired,
    min(case when payment_status = "merchantAcquiredPayment" then day end) as merchant_acquired_payment
from 
(select distinct _id, orderId, merchantId, manDays, daysAfterQC, day, s.status as payment_status
from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }} o
left join statuses s on o._id = s.id
)
group by _id, orderId, merchantId
)
),

order_statuses as 
    (
        select 
        order_id,
        MIN(case when sub_status = "signingAndPayment" then o.event_ts_msk end) as signing_and_payment,
        MIN(case when status = "manufacturing" then o.event_ts_msk end) as manufacturing,
        MIN(case when status = "cancelled" then o.event_ts_msk end) as cancelled,
        MIN(case when status = "shipping" then o.event_ts_msk end) as shipping,
        MIN(case when status = "claim" then o.event_ts_msk end) as claimed,
        MIN(case when status = "closed" then o.event_ts_msk end) as closed
    FROM {{ ref('fact_order_change') }} o
    group by order_id
)

select 
    merchant_order_id, 
    merchant_orders.order_id, 
    merchant_id, 
    longest_order, 
    one_merchant_order, 
    date(signing_and_payment) as signing_and_payment,
    date(manufacturing) as manufacturing,
    date(no_operations_started) as no_operations_started,
    date(advance_payment_requested) as advance_payment_requested,
    date(advance_payment_in_progress) as advance_payment_in_progress,
    date(advance_payment_acquired) as advance_payment_acquired,
    date(manufacturing_and_qc_in_progress) as manufacturing_and_qc_in_progress,
    date(remaining_payment_requested) as remaining_payment_requested,
    date(remaining_payment_in_progress) as remaining_payment_in_progress,
    date(remaining_payment_acquired) as remaining_payment_acquired,
    date(complete_payment_requested) as complete_payment_requested,
    date(complete_payment_in_progress) as complete_payment_in_progress,
    date(complete_payment_acquired) as complete_payment_acquired,
    date(merchant_acquired_payment) as merchant_acquired_payment,
    case when advance_payment_requested is not null then 'advance' else 'complete' end as payment_type,
    
    coalesce(shipping, closed) as manufacturing_ended,
    man_days,
        
    case when claimed is not null then 'claimed'
        when cancelled is not null and cancelled > manufacturing then 'cancelled' 
        when closed is not null then 'ok' end as claim
from merchant_orders 
left join order_statuses on merchant_orders.order_id = order_statuses.order_id
