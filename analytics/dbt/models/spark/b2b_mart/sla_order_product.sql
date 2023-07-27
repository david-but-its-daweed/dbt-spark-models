{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH product_statuses AS (
    SELECT
        merchant_order_id,
        product_id,
        min(CASE WHEN status = 'formingOrder' THEN event_ts_msk END) AS forming_order,
        min(CASE WHEN status = 'unknown' THEN event_ts_msk END) AS product_unknown,
        min(CASE WHEN status = 'manufacturing' THEN event_ts_msk END) AS product_manufacturing,
        min(CASE WHEN status = 'pickUp' THEN event_ts_msk END) AS product_pick_up,
        min(CASE WHEN status = 'psi' THEN event_ts_msk END) AS product_psi,
        min(CASE WHEN status = 'signingWithMerch' THEN event_ts_msk END) AS signing_with_merchant,
        min(CASE WHEN status = 'orderCompleted' THEN event_ts_msk END) AS order_completed,
        min(CASE WHEN status = 'cancelled' THEN event_ts_msk END) AS cancelled,
        min(CASE WHEN status = 'readyForPsi' THEN event_ts_msk END) AS ready_for_psi,
        min(CASE WHEN status = 'pickupRequested' THEN event_ts_msk END) AS pickup_requested,
        min(CASE WHEN status = 'readyForShipment' THEN event_ts_msk END) AS ready_for_shipment,
        min(CASE WHEN status = 'pickupCompleted' THEN event_ts_msk END) AS pickup_completed,
        max(CASE WHEN event_ts_msk = rn THEN status END) AS product_status
    FROM (
        SELECT
            merchant_order_id,
            product_id,
            status,
            event_ts_msk,
            max(event_ts_msk) OVER (PARTITION BY merchant_order_id, product_id) AS rn
        FROM {{ ref('statuses_events') }}
        WHERE entity = 'product'
    )
    GROUP BY
        merchant_order_id,
        product_id
),

merchant_orders AS (
    SELECT
        merchant_order_id,
        merchant_order_status,
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
    FROM (
        SELECT
            merchant_order_id,
            min(date(CASE WHEN status = 'noOperationsStarted' THEN event_ts_msk END)) AS no_operations_started,
            min(date(CASE WHEN status = 'advancePaymentRequested' THEN event_ts_msk END)) AS advance_payment_requested,
            min(date(CASE WHEN status = 'advancePaymentInProgress' THEN event_ts_msk END)) AS advance_payment_in_progress,
            min(date(CASE WHEN status = 'advancePaymentAcquired' THEN event_ts_msk END)) AS advance_payment_acquired,
            min(date(CASE WHEN status = 'manufacturingAndQcInProgress' THEN event_ts_msk END)) AS manufacturing_and_qc_in_progress,
            min(date(CASE WHEN status = 'remainingPaymentRequested' THEN event_ts_msk END)) AS remaining_payment_requested,
            min(date(CASE WHEN status = 'remainingPaymentInProgress' THEN event_ts_msk END)) AS remaining_payment_in_progress,
            min(date(CASE WHEN status = 'remainingPaymentAcquired' THEN event_ts_msk END)) AS remaining_payment_acquired,
            min(date(CASE WHEN status = 'completePaymentRequested' THEN event_ts_msk END)) AS complete_payment_requested,
            min(date(CASE WHEN status = 'completePaymentInProgress' THEN event_ts_msk END)) AS complete_payment_in_progress,
            min(date(CASE WHEN status = 'completePaymentAcquired' THEN event_ts_msk END)) AS complete_payment_acquired,
            min(date(CASE WHEN status = 'merchantAcquiredPayment' THEN event_ts_msk END)) AS merchant_acquired_payment,
            max(CASE WHEN event_ts_msk = rn THEN status END) AS merchant_order_status
        FROM (
            SELECT
                *,
                max(event_ts_msk) OVER (PARTITION BY merchant_order_id) AS rn
            FROM {{ ref('statuses_events') }}
            WHERE entity = 'merchant order'
        )
        GROUP BY merchant_order_id
    )
),

order_products AS (
    SELECT
        user_id,
        deal_id,
        deal_type,
        deal_name,
        order_id,
        order_friendly_id,
        merchant_order_id,
        merchant_order_friendly_id,
        offer_product_id,
        offer_id,
        product_type,
        offer_type,
        offer_status,
        customer_request_id,
        product_id,
        final_gmv,
        psi_status_id,
        psi_status,
        psi_conducted,
        product_price,
        merchant_id,
        merchant_type,
        current_order_status,
        current_order_substatus,
        min_status_selling_ts_msk,
        min_status_manufacturing_ts_msk,
        min_status_shipping_ts_msk,
        min_status_cancelled_ts_msk,
        declared_manufacturing_days,
        owner_email,
        owner_role
    FROM {{ ref('fact_order_product_deal') }}
),

admins AS (
    SELECT DISTINCT
        admin_id,
        email,
        role
    FROM {{ ref('dim_user_admin') }}
),

biz_dev AS (
    SELECT
        fo.biz_dev_id,
        a.email AS biz_dev_email,
        a.role AS biz_dev_role,
        fo.order_id
    FROM {{ ref('fact_order') }} AS fo
    LEFT JOIN admins AS a ON fo.biz_dev_id = a.admin_id
    WHERE fo.next_effective_ts_msk IS NULL
),

psi AS (
    SELECT
        merchant_order_id,
        product_id,
        max(running_time) AS psi_running_time,
        max(ready_time) AS psi_ready_time,
        max(coalesce(failed_time, psi_end_time)) AS psi_end_time
    FROM {{ ref('fact_psi') }}
    GROUP BY merchant_order_id, product_id
),

order_statuses AS (
    SELECT
        order_id,
        max(CASE WHEN sub_status = 'joomSIAPaymentReceived' THEN event_ts_msk END) AS sia_payment_received,
        max(CASE WHEN sub_status = 'client2BrokerPaymentSent' THEN event_ts_msk END) AS client_to_broker_payment_sent,
        max(CASE WHEN sub_status = 'brokerPaymentReceived' THEN event_ts_msk END) AS broker_payment_received,
        max(CASE WHEN sub_status = 'broker2JoomSIAPaymentSent' THEN event_ts_msk END) AS broker_to_sia_payment_sent
    FROM {{ ref('fact_order_statuses') }}
    GROUP BY order_id
)


SELECT DISTINCT
    op.user_id,
    op.deal_id,
    op.deal_type,
    op.deal_name,
    op.order_id,
    op.order_friendly_id,
    op.merchant_order_id,
    op.merchant_order_friendly_id,
    op.offer_product_id,
    op.offer_id,
    op.product_type,
    op.offer_type,
    op.offer_status,
    op.customer_request_id,
    op.product_id,
    op.final_gmv,
    op.psi_status_id,
    op.psi_status,
    op.psi_conducted,
    op.product_price,
    op.merchant_id,
    op.merchant_type,
    op.declared_manufacturing_days,
    op.owner_email,
    op.owner_role,
    bd.biz_dev_id,
    bd.biz_dev_email,
    bd.biz_dev_role,
    mo.merchant_order_status,
    mo.no_operations_started,
    mo.advance_payment_requested,
    mo.advance_payment_in_progress,
    mo.advance_payment_acquired,
    mo.manufacturing_and_qc_in_progress,
    mo.remaining_payment_requested,
    mo.remaining_payment_in_progress,
    mo.remaining_payment_acquired,
    mo.complete_payment_requested,
    mo.complete_payment_in_progress,
    mo.complete_payment_acquired,
    mo.merchant_acquired_payment,
    ps.forming_order,
    ps.product_unknown,
    ps.product_manufacturing,
    ps.product_pick_up,
    ps.product_psi,
    ps.signing_with_merchant,
    ps.order_completed,
    ps.cancelled,
    ps.ready_for_psi,
    ps.pickup_requested,
    ps.ready_for_shipment,
    ps.pickup_completed,
    psi.psi_running_time,
    psi.psi_ready_time,
    psi.psi_end_time,
    op.current_order_status,
    op.current_order_substatus,
    op.min_status_selling_ts_msk,
    op.min_status_manufacturing_ts_msk,
    op.min_status_shipping_ts_msk,
    op.min_status_cancelled_ts_msk,
    os.sia_payment_received,
    os.client_to_broker_payment_sent,
    os.broker_payment_received,
    os.broker_to_sia_payment_sent
FROM order_products AS op
LEFT JOIN merchant_orders AS mo ON mo.merchant_order_id = op.merchant_order_id
LEFT JOIN product_statuses AS ps ON ps.merchant_order_id = mo.merchant_order_id AND ps.product_id = op.product_id
LEFT JOIN biz_dev AS bd ON op.order_id = bd.order_id
LEFT JOIN psi ON op.merchant_order_id = psi.merchant_order_id AND psi.product_id = op.product_id
LEFT JOIN order_statuses AS os ON op.order_id = os.order_id
