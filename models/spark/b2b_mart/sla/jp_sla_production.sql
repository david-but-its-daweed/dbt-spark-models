{{ config(
    schema='b2b_mart',
    materialized='view',
    meta = {
      'model_owner' : '@amitiushkina',
      'team': 'general_analytics',
      'bigquery_load': 'true',
      'priority_weight': '150'
    }
) }}


WITH product_statuses AS (
    SELECT *
    FROM {{ ref('fact_order_product_statuses') }}
),

merchant_orders AS (
    SELECT *
    FROM {{ ref('fact_merchant_order_statuses') }}
),

order_statuses AS (
    SELECT *
    FROM {{ ref('fact_order_statuses') }}
),

psi AS (
    SELECT
        merchant_order_id,
        product_id,
        max(running_time) AS psi_running_time,
        max(ready_time) AS psi_ready_time,
        max(failed_time) AS psi_failed_time,
        max(psi_end_time) AS psi_end_time,
        max(psi_start) as date_of_inspection
    FROM {{ ref('fact_psi') }}
    GROUP BY merchant_order_id, product_id
),

pickup_orders as (
    select 
      pickup_id,
      pickup_friendly_id,
      merchant_order_id,
      order_id,
      arrived_date,
      pickup_date,
      planned_date,
      shipped_date,
      min(case when statuses.status = 'WaitingForConfirmation' then time end) as waiting_for_confirmation,
      min(case when statuses.status = 'Requested' then time end) as pickup_requested,
      min(case when statuses.status = 'PickedUp' then time end) as picked_up
    from {{ ref('fact_pickup_order') }}
    group by
      pickup_id,
      pickup_friendly_id,
      merchant_order_id,
      order_id,
      arrived_date,
      pickup_date,
      planned_date,
      shipped_date
),

order_products AS (
    SELECT
        deal_id,
        user_id,
        customer_request_id,
        offer_id,
        offer_product_id,
        product_id,
        order_id,
        order_friendly_id,
        merchant_order_id,
        merchant_order_friendly_id,
        merchant_id,
        order_product_id,
        product_friendly_id,
        owner_email,
        owner_role
    FROM {{ ref('dim_deal_products') }}
)

SELECT DISTINCT
    op.*,
    pickup_id,
    pickup_friendly_id,
    signing_and_payment,
    manufacturing,
    joom_sia_payment_received,
    advance_payment_requested,
    advance_payment_in_progress,
    advance_payment_acquired,
    manufacturing_and_qc_in_progress,
    remaining_payment_requested,
    remaining_payment_in_progress,
    remaining_payment_acquired,
    signing_with_merchant,
    awaiting_manufacturing,
    manufacturing,
    psi,
    pick_up,
    order_completed,
    cancelled,
    date_of_inspection,
    psi_running_time,
    psi_ready_time,
    psi_failed_time,
    psi_end_time,
    pickup_date,
    planned_date AS pickup_planned_date,
    waiting_for_confirmation AS pickup_waiting_for_confirmation,
    pickup_requested,
    mo.last_status as merchant_order_status,
    op.last_status as product_status,
    current_status,
    current_sub_status
FROM      order_products   AS op
LEFT JOIN product_statuses AS ps   USING (product_id, merchant_order_id)
LEFT JOIN psi                      USING (product_id, merchant_order_id)
LEFT JOIN order_statuses   AS os   USING (order_id)
LEFT JOIN merchant_orders  AS mo   USING (merchant_order_id)
LEFT JOIN pickup_orders    AS pu   USING (order_id, merchant_order_id)
