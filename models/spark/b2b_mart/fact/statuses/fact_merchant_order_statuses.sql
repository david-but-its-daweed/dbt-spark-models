{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}
  


WITH merchant_orders AS (
    SELECT
        deal_id,
        user_id,
        customer_request_id,
        offer_id,
        offer_product_id,
        order_id,
        order_friendly_id,
        merchant_order_id,
        merchant_order_friendly_id,
        merchant_id,
        owner_email,
        owner_role
    FROM {{ ref('dim_deal_products') }}
),


statuses as 
(select
    merchant_order_id,
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
    merchant_acquired_payment,
    last_status
    from
(
select 
    merchant_order_id, 
    max(last_status) as last_status,
    min(date(case when payment_status = "noOperationsStarted" then event_ts_msk end)) as no_operations_started,
    min(date(case when payment_status = "advancePaymentRequested" then event_ts_msk end)) as advance_payment_requested,
    min(date(case when payment_status = "advancePaymentInProgress" then event_ts_msk end)) as advance_payment_in_progress,
    min(date(case when payment_status = "advancePaymentAcquired" then event_ts_msk end)) as advance_payment_acquired,
    min(date(case when payment_status = "manufacturingAndQCInProgress" then event_ts_msk end)) as manufacturing_and_qc_in_progress,
    min(date(case when payment_status = "remainingPaymentRequested" then event_ts_msk end)) as remaining_payment_requested,
    min(date(case when payment_status = "remainingPaymentInProgress" then event_ts_msk end)) as remaining_payment_in_progress,
    min(date(case when payment_status = "remainingPaymentAcquired" then event_ts_msk end)) as remaining_payment_acquired,
    min(date(case when payment_status = "completePaymentRequested" then event_ts_msk end)) as complete_payment_requested,
    min(date(case when payment_status = "completePaymentInProgress" then event_ts_msk end)) as complete_payment_in_progress,
    min(date(case when payment_status = "completePaymentAcquired" then event_ts_msk end)) as complete_payment_acquired,
    min(date(case when payment_status = "merchantAcquiredPayment" then event_ts_msk end)) as merchant_acquired_payment
from 
(
        select *, first_value(payment_status) over (partition by merchant_order_id order by event_ts_msk desc) as last_status
        from
    (
    select _id as merchant_order_id, 
        status.status as payment_status,
        min(TIMESTAMP(millis_to_ts_msk(coalesce(col.statusDate, col.utms)))) as event_ts_msk
        from
        (
        select _id, explode(payment.paymentStatusHistory)
        from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
        ) history
        left join {{ ref('key_payment_status') }} status on col.paymentStatus = status.status_int
        group by 1, 2
    )
    )
group by merchant_order_id
)
)


select 
    mo.deal_id,
    mo.user_id,
    mo.customer_request_id,
    mo.offer_id,
    mo.offer_product_id,
    mo.order_id,
    mo.order_friendly_id,
    mo.merchant_order_id,
    mo.merchant_order_friendly_id,
    mo.merchant_id,
    mo.owner_email,
    mo.owner_role,
    count(merchant_order_id) over (partition by order_id) = 1 as one_merchant_order,
    last_status,
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
from merchant_orders mo
join statuses s using (merchant_order_id)
