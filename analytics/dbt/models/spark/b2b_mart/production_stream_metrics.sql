{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'bigquery_load': 'true'
    }
) }}

with payments as (
  select 10 as id, "noOperationsStarted" as status
  union all 
  select 20 as id, "advancePaymentRequested" as status
  union all
  select 25 as id, "advancePaymentInProgress" as status
  union all
  select 27 as id, "advancePaymentAcquired" as status
  union all
  select 30 as id, "manufacturingAndQcInProgress" as status
  union all
  select 40 as id, "remainingPaymentRequested" as status
  union all
  select 45 as id, "remainingPaymentInProgress" as status
  union all
  select 50 as id, "remainingPaymentAcquired" as status
  union all
  select 60 as id, "completePaymentRequested" as status
  union all
  select 65 as id, "completePaymentInProgress" as status
  union all
  select 70 as id, "completePaymentAcquired" as status
  union all
  select 80 as id, "merchantAcquiredPayment" as status
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
    min(case when payment_status = 10 then day end) as no_operations_started,
    min(case when payment_status = 20 then day end) as advance_payment_requested,
    min(case when payment_status = 25 then day end) as advance_payment_in_progress,
    min(case when payment_status = 27 then day end) as advance_payment_acquired,
    min(case when payment_status = 30 then day end) as manufacturing_and_qc_in_progress,
    min(case when payment_status = 40 then day end) as remaining_payment_requested,
    min(case when payment_status = 45 then day end) as remaining_payment_in_progress,
    min(case when payment_status = 50 then day end) as remaining_payment_acquired,
    min(case when payment_status = 60 then day end) as complete_payment_requested,
    min(case when payment_status = 65 then day end) as complete_payment_in_progress,
    min(case when payment_status = 70 then day end) as complete_payment_acquired,
    min(case when payment_status = 80 then day end) as merchant_acquired_payment
from 
(select _id, orderId, merchantId, manDays, daysAfterQC, day, status, payment_status
from
(select _id, orderId, merchantId, manDays, daysAfterQC, from_unixtime(status.utms/1000 + 10800) as day,  status.paymentStatus as payment_status
from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }} o
lateral view explode(payment.paymentStatusHistory) as status) o
left join payments p on o.payment_status = p.id
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
