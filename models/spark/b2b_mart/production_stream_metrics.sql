{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}

with statuses as (
    select _id as id, 
        status.status as status,
        min(TIMESTAMP(millis_to_ts_msk(coalesce(col.statusDate, col.utms)))) as day
        from
        (
        select _id, explode(payment.paymentStatusHistory)
        from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }}
        ) history
        left join {{ ref('key_payment_status') }} status on col.paymentStatus = status.status_int
        group by 1, 2
    )
,

     added_data AS (
    select
    'XN2QX_XE6J3' AS friendly_id,
    '15.07.2022' AS advance_payment_requested,
    '19.07.2022' AS advance_payment_in_progress,
    '19.07.2022' AS advance_payment_acquired,
    5 AS manufacturing_days,
    '20.07.2022' AS ready_for_psi,
    '23.07.2022' AS psi_ready,
    '23.07.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '01.08.2022' AS remaining_payment_in_progress,
    '01.08.2022' AS remaining_payment_acquired,
    '03.08.2022' AS shipped
    union all
    select
    'XN2QX_XZZVX' AS friendly_id,
    '15.07.2022' AS advance_payment_requested,
    '19.07.2022' AS advance_payment_in_progress,
    '21.07.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '29.07.2022' AS ready_for_psi,
    '29.07.2022' AS psi_ready,
    '01.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '01.08.2022' AS remaining_payment_in_progress,
    '02.08.2022' AS remaining_payment_acquired,
    '03.08.2022' AS shipped
    union all
    select
    '3W27G' AS friendly_id,
    '07.07.2022' AS advance_payment_requested,
    '11.07.2022' AS advance_payment_in_progress,
    '12.07.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '31.07.2022' AS ready_for_psi,
    '03.08.2022' AS psi_ready,
    '08.08.2022' AS client_gave_feedback,
    '08.08.2022' AS problems_are_fixed,
    '10.08.2022' AS remaining_payment_in_progress,
    '11.08.2022' AS remaining_payment_acquired,
    '15.08.2022' AS shipped
    union all
    select
    'XJKWQ' AS friendly_id,
    '28.07.2022' AS advance_payment_requested,
    '28.07.2022' AS advance_payment_in_progress,
    '29.07.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '08.08.2022' AS ready_for_psi,
    '09.08.2022' AS psi_ready,
    '11.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '11.08.2022' AS remaining_payment_in_progress,
    '15.08.2022' AS remaining_payment_acquired,
    '16.08.2022' AS shipped
    union all
    select
    'XGNWM' AS friendly_id,
    '12.08.2022' AS advance_payment_requested,
    '08.08.2022' AS advance_payment_in_progress,
    '10.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '14.08.2022' AS ready_for_psi,
    '15.08.2022' AS psi_ready,
    '16.08.2022' AS client_gave_feedback,
    '16.08.2022' AS problems_are_fixed,
    '18.08.2022' AS remaining_payment_in_progress,
    '19.08.2022' AS remaining_payment_acquired,
    '21.08.2022' AS shipped
    union all
    select
    'XNW7Q' AS friendly_id,
    '25.08.2022' AS advance_payment_requested,
    '05.08.2022' AS advance_payment_in_progress,
    '10.08.2022' AS advance_payment_acquired,
    17 AS manufacturing_days,
    '29.08.2022' AS ready_for_psi,
    '29.08.2022' AS psi_ready,
    '29.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '30.08.2022' AS remaining_payment_in_progress,
    '31.08.2022' AS remaining_payment_acquired,
    '01.09.2022' AS shipped
    union all
    select
    'KRE4P' AS friendly_id,
    '26.08.2022' AS advance_payment_requested,
    '22.08.2022' AS advance_payment_in_progress,
    '23.08.2022' AS advance_payment_acquired,
    30 AS manufacturing_days,
    '01.09.2022' AS ready_for_psi,
    '02.09.2022' AS psi_ready,
    '05.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '' AS remaining_payment_in_progress,
    '' AS remaining_payment_acquired,
    '07.09.2022' AS shipped
    union all
    select
    'KWW72_YLLPW' AS friendly_id,
    '25.08.2022' AS advance_payment_requested,
    '25.08.2022' AS advance_payment_in_progress,
    '25.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '31.08.2022' AS ready_for_psi,
    '31.08.2022' AS psi_ready,
    '01.09.2022' AS client_gave_feedback,
    '01.09.2022' AS problems_are_fixed,
    '06.09.2022' AS remaining_payment_in_progress,
    '06.09.2022' AS remaining_payment_acquired,
    '09.09.2022' AS shipped
    union all
    select
    'KWW72_YNN2L' AS friendly_id,
    '25.08.2022' AS advance_payment_requested,
    '25.08.2022' AS advance_payment_in_progress,
    '25.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '01.09.2022' AS ready_for_psi,
    '01.09.2022' AS psi_ready,
    '01.09.2022' AS client_gave_feedback,
    '05.09.2022' AS problems_are_fixed,
    '06.09.2022' AS remaining_payment_in_progress,
    '06.09.2022' AS remaining_payment_acquired,
    '09.09.2022' AS shipped
    union all
    select
    'XEJK3' AS friendly_id,
    '20.06.2022' AS advance_payment_requested,
    '21.06.2022' AS advance_payment_in_progress,
    '22.06.2022' AS advance_payment_acquired,
    60 AS manufacturing_days,
    '16.08.2022' AS ready_for_psi,
    '19.08.2022' AS psi_ready,
    '22.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '22.08.2022' AS remaining_payment_in_progress,
    '23.08.2022' AS remaining_payment_acquired,
    '10.09.2022' AS shipped
    union all
    select
    'X5RV3' AS friendly_id,
    '30.06.2022' AS advance_payment_requested,
    '01.07.2022' AS advance_payment_in_progress,
    '03.07.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '08.08.2022' AS ready_for_psi,
    '08.08.2022' AS psi_ready,
    '11.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '11.08.2022' AS remaining_payment_in_progress,
    '13.08.2022' AS remaining_payment_acquired,
    '10.09.2022' AS shipped
    union all
    select
    'J39RY' AS friendly_id,
    '31.08.2022' AS advance_payment_requested,
    '31.08.2022' AS advance_payment_in_progress,
    '31.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '10.09.2022' AS ready_for_psi,
    '12.09.2022' AS psi_ready,
    '14.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '14.09.2022' AS remaining_payment_in_progress,
    '14.09.2022' AS remaining_payment_acquired,
    '15.09.2022' AS shipped
    union all
    select
    'XQMDY' AS friendly_id,
    '07.09.2022' AS advance_payment_requested,
    '30.08.2022' AS advance_payment_in_progress,
    '01.09.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '06.09.2022' AS ready_for_psi,
    '07.09.2022' AS psi_ready,
    '07.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '12.09.2022' AS remaining_payment_in_progress,
    '14.09.2022' AS remaining_payment_acquired,
    '15.09.2022' AS shipped
    union all
    select
    'KE7XR' AS friendly_id,
    '22.08.2022' AS advance_payment_requested,
    '22.08.2022' AS advance_payment_in_progress,
    '23.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '04.09.2022' AS ready_for_psi,
    '05.09.2022' AS psi_ready,
    '07.09.2022' AS client_gave_feedback,
    '13.09.2022' AS problems_are_fixed,
    '14.09.2022' AS remaining_payment_in_progress,
    '14.09.2022' AS remaining_payment_acquired,
    '16.09.2022' AS shipped
    union all
    select
    '3D4GL' AS friendly_id,
    '29.08.2022' AS advance_payment_requested,
    '30.08.2022' AS advance_payment_in_progress,
    '31.08.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '06.09.2022' AS ready_for_psi,
    '06.09.2022' AS psi_ready,
    '06.09.2022' AS client_gave_feedback,
    '13.09.2022' AS problems_are_fixed,
    '15.09.2022' AS remaining_payment_in_progress,
    '16.09.2022' AS remaining_payment_acquired,
    '17.09.2022' AS shipped
    union all
    select
    'J2QYV' AS friendly_id,
    '15.09.2022' AS advance_payment_requested,
    '15.09.2022' AS advance_payment_in_progress,
    '16.09.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '16.09.2022' AS ready_for_psi,
    '20.09.2022' AS psi_ready,
    '20.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '05.10.2022' AS remaining_payment_in_progress,
    '06.10.2022' AS remaining_payment_acquired,
    '21.09.2022' AS shipped
    union all
    select
    'X49KL' AS friendly_id,
    '26.08.2022' AS advance_payment_requested,
    '26.08.2022' AS advance_payment_in_progress,
    '26.08.2022' AS advance_payment_acquired,
    30 AS manufacturing_days,
    '16.09.2022' AS ready_for_psi,
    '16.09.2022' AS psi_ready,
    '19.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '19.09.2022' AS remaining_payment_in_progress,
    '19.09.2022' AS remaining_payment_acquired,
    '21.09.2022' AS shipped
    union all
    select
    'KMQ98' AS friendly_id,
    '26.08.2022' AS advance_payment_requested,
    '30.08.2022' AS advance_payment_in_progress,
    '01.09.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '19.09.2022' AS ready_for_psi,
    '20.09.2022' AS psi_ready,
    '20.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '21.09.2022' AS remaining_payment_in_progress,
    '22.09.2022' AS remaining_payment_acquired,
    '23.09.2022' AS shipped
    union all
    select
    'KGLEL' AS friendly_id,
    '22.08.2022' AS advance_payment_requested,
    '22.08.2022' AS advance_payment_in_progress,
    '24.08.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '19.09.2022' AS ready_for_psi,
    '19.09.2022' AS psi_ready,
    '20.09.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '22.09.2022' AS remaining_payment_in_progress,
    '26.09.2022' AS remaining_payment_acquired,
    '27.09.2022' AS shipped
    union all
    select
    '3KR5X' AS friendly_id,
    '29.08.2022' AS advance_payment_requested,
    '06.09.2022' AS advance_payment_in_progress,
    '07.09.2022' AS advance_payment_acquired,
    20 AS manufacturing_days,
    '27.08.2022' AS ready_for_psi,
    '28.08.2022' AS psi_ready,
    '28.08.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '30.09.2022' AS remaining_payment_in_progress,
    '30.09.2022' AS remaining_payment_acquired,
    '08.10.2022' AS shipped
    union all
    select
    'J4QE6' AS friendly_id,
    '20.09.2022' AS advance_payment_requested,
    '20.09.2022' AS advance_payment_in_progress,
    '21.09.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '10.10.2022' AS ready_for_psi,
    '10.10.2022' AS psi_ready,
    '10.10.2022' AS client_gave_feedback,
    '18.10.2022' AS problems_are_fixed,
    '18.10.2022' AS remaining_payment_in_progress,
    '19.10.2022' AS remaining_payment_acquired,
    '21.10.2022' AS shipped
    union all
    select
    'J2Q29' AS friendly_id,
    '20.09.2022' AS advance_payment_requested,
    '20.09.2022' AS advance_payment_in_progress,
    '22.09.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '17.10.2022' AS ready_for_psi,
    '17.10.2022' AS psi_ready,
    '18.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '19.10.2022' AS remaining_payment_in_progress,
    '21.10.2022' AS remaining_payment_acquired,
    '22.10.2022' AS shipped
    union all
    select
    'K6DZX' AS friendly_id,
    '22.09.2022' AS advance_payment_requested,
    '26.09.2022' AS advance_payment_in_progress,
    '28.09.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '10.10.2022' AS ready_for_psi,
    '11.10.2022' AS psi_ready,
    '11.10.2022' AS client_gave_feedback,
    '12.10.2022' AS problems_are_fixed,
    '13.10.2022' AS remaining_payment_in_progress,
    '14.10.2022' AS remaining_payment_acquired,
    '23.10.2022' AS shipped
    union all
    select
    'JV349' AS friendly_id,
    '29.09.2022' AS advance_payment_requested,
    '30.09.2022' AS advance_payment_in_progress,
    '01.10.2022' AS advance_payment_acquired,
    22 AS manufacturing_days,
    '19/10/2022' AS ready_for_psi,
    '20.10.2022' AS psi_ready,
    '20.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '20.10.2022' AS remaining_payment_in_progress,
    '24.10.2022' AS remaining_payment_acquired,
    '25.10.2022' AS shipped
    union all
    select
    'KDX4M_Y4VE9' AS friendly_id,
    '05.10.2022' AS advance_payment_requested,
    '06.10.2022' AS advance_payment_in_progress,
    '06.10.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '22.10.2022' AS ready_for_psi,
    '24.10.2022' AS psi_ready,
    '24.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '24.10.2022' AS remaining_payment_in_progress,
    '25.10.2022' AS remaining_payment_acquired,
    '26.10.2022' AS shipped
    union all
    select
    'KDX4M_G57QX' AS friendly_id,
    '05.10.2022' AS advance_payment_requested,
    '06.10.2022' AS advance_payment_in_progress,
    '06.10.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '12.10.2022' AS ready_for_psi,
    '14.10.2022' AS psi_ready,
    '17.10.2022' AS client_gave_feedback,
    '19.10.2022' AS problems_are_fixed,
    '19.10.2022' AS remaining_payment_in_progress,
    '19.10.2022' AS remaining_payment_acquired,
    '26.10.2022' AS shipped
    union all
    select
    'JZYL9' AS friendly_id,
    '29.08.2022' AS advance_payment_requested,
    '06.09.2022' AS advance_payment_in_progress,
    '06.09.2022' AS advance_payment_acquired,
    20 AS manufacturing_days,
    '27.09.2022' AS ready_for_psi,
    '19.10.2022' AS psi_ready,
    '20.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '20.10.2022' AS remaining_payment_in_progress,
    '20.10.2022' AS remaining_payment_acquired,
    '26.10.2022' AS shipped
    union all
    select
    'K5VPE' AS friendly_id,
    '03.10.2022' AS advance_payment_requested,
    '04.10.2022' AS advance_payment_in_progress,
    '06.10.2022' AS advance_payment_acquired,
    20 AS manufacturing_days,
    '20.10.2022' AS ready_for_psi,
    '21.10.2022' AS psi_ready,
    '24.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '24.10.2022' AS remaining_payment_in_progress,
    '25.10.2022' AS remaining_payment_acquired,
    '27.10.2022' AS shipped
    union all
    select
    'K5V88' AS friendly_id,
    '07.10.2022' AS advance_payment_requested,
    '13.10.2022' AS advance_payment_in_progress,
    '14.10.2022' AS advance_payment_acquired,
    7 AS manufacturing_days,
    '24.10.2022' AS ready_for_psi,
    '24.10.2022' AS psi_ready,
    '25.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '26.10.2022' AS remaining_payment_in_progress,
    '27.10.2022' AS remaining_payment_acquired,
    '28.10.2022' AS shipped
    union all
    select
    'KGRDV' AS friendly_id,
    '05.10.2022' AS advance_payment_requested,
    '06.10.2022' AS advance_payment_in_progress,
    '11.10.2022' AS advance_payment_acquired,
    11 AS manufacturing_days,
    '24.10.2022' AS ready_for_psi,
    '24.10.2022' AS psi_ready,
    '27.10.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '27.10.2022' AS remaining_payment_in_progress,
    '31.10.2022' AS remaining_payment_acquired,
    '31.10.2022' AS shipped
    union all
    select
    'JQZ5M' AS friendly_id,
    '14.10.2022' AS advance_payment_requested,
    '17.10.2022' AS advance_payment_in_progress,
    '19.10.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '29.10.2022' AS ready_for_psi,
    '03.11.2022' AS psi_ready,
    '03.11.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '07.11.2022' AS remaining_payment_in_progress,
    '09.11.2022' AS remaining_payment_acquired,
    '10.11.2022' AS shipped
    union all
    select
    'JV3XE' AS friendly_id,
    '19.10.2022' AS advance_payment_requested,
    '20.10.2022' AS advance_payment_in_progress,
    '21.10.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '03.11.2022' AS ready_for_psi,
    '03.11.2022' AS psi_ready,
    '08.11.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '08.11.2022' AS remaining_payment_in_progress,
    '09.11.2022' AS remaining_payment_acquired,
    '10.11.2022' AS shipped
    union all
    select
    '3DDLN' AS friendly_id,
    '03.10.2022' AS advance_payment_requested,
    '30.09.2022' AS advance_payment_in_progress,
    '10.10.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '20.10.2022' AS ready_for_psi,
    '20.10.2022' AS psi_ready,
    '21.10.2022' AS client_gave_feedback,
    '07.11.2022' AS problems_are_fixed,
    '20.10.2022' AS remaining_payment_in_progress,
    '21.10.2022' AS remaining_payment_acquired,
    '12.11.2022' AS shipped
    union all
    select
    'J9979' AS friendly_id,
    '31.10.2022' AS advance_payment_requested,
    '28.10.2022' AS advance_payment_in_progress,
    '31.10.2022' AS advance_payment_acquired,
    10 AS manufacturing_days,
    '10.11.2022' AS ready_for_psi,
    '10.11.2022' AS psi_ready,
    '14.11.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '14.11.2022' AS remaining_payment_in_progress,
    '16.11.2022' AS remaining_payment_acquired,
    '18.11.2022' AS shipped
    union all
    select
    'KLGNE' AS friendly_id,
    '07.10.2022' AS advance_payment_requested,
    '10.10.2022' AS advance_payment_in_progress,
    '11.10.2022' AS advance_payment_acquired,
    30 AS manufacturing_days,
    '07.11.2022' AS ready_for_psi,
    '07.11.2022' AS psi_ready,
    '08.11.2022' AS client_gave_feedback,
    '09.11.2022' AS problems_are_fixed,
    '10.11.2022' AS remaining_payment_in_progress,
    '12.11.2022' AS remaining_payment_acquired,
    '22.11.2022' AS shipped
    union all
    select
    'KLZWL' AS friendly_id,
    '31.10.2022' AS advance_payment_requested,
    '28.10.2022' AS advance_payment_in_progress,
    '30.10.2022' AS advance_payment_acquired,
    15 AS manufacturing_days,
    '15.11.2022' AS ready_for_psi,
    '16.11.2022' AS psi_ready,
    '17.11.2022' AS client_gave_feedback,
    '21.11.2022' AS problems_are_fixed,
    '23.11.2022' AS remaining_payment_in_progress,
    '24.11.2022' AS remaining_payment_acquired,
    '25.11.2022' AS shipped
    union all
    select
    'JYDE2' AS friendly_id,
    '05.10.2022' AS advance_payment_requested,
    '06.10.2022' AS advance_payment_in_progress,
    '11.10.2022' AS advance_payment_acquired,
    16 AS manufacturing_days,
    '27.10.2022' AS ready_for_psi,
    '21.11.2022' AS psi_ready,
    '23.11.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '23.11.2022' AS remaining_payment_in_progress,
    '24.11.2022' AS remaining_payment_acquired,
    '26.11.2022' AS shipped
    union all
    select
    'JNEVD' AS friendly_id,
    '31.10.2022' AS advance_payment_requested,
    '28.10.2022' AS advance_payment_in_progress,
    '29.10.2022' AS advance_payment_acquired,
    20 AS manufacturing_days,
    '21.11.2022' AS ready_for_psi,
    '21.11.2022' AS psi_ready,
    '22.11.2022' AS client_gave_feedback,
    '' AS problems_are_fixed,
    '23.11.2022' AS remaining_payment_in_progress,
    '24.11.2022' AS remaining_payment_acquired,
    '30.11.2022' AS shipped   
),

added_orders as (
    select 
    friendly_id as merchant_order_id, 
    orderId as order_id,
    merchantId as merchant_id, 
    max(last_payment_status) as last_payment_status,
    max(manDays) as man_days,
    min(case when payment_status = "noOperationsStarted" then day end) as no_operations_started,
    min(to_date(advance_payment_requested, 'dd.MM.yyyy')) as advance_payment_requested,
    min(to_date(advance_payment_in_progress, 'dd.MM.yyyy')) as advance_payment_in_progress,
    min(to_date(advance_payment_acquired, 'dd.MM.yyyy')) as advance_payment_acquired,
    min(case when payment_status = "manufacturingAndQcInProgress" then day end) as manufacturing_and_qc_in_progress,
    min(to_date(client_gave_feedback, 'dd.MM.yyyy')) as remaining_payment_requested,
    min(to_date(remaining_payment_in_progress, 'dd.MM.yyyy')) as remaining_payment_in_progress,
    min(to_date(remaining_payment_acquired, 'dd.MM.yyyy')) as remaining_payment_acquired,
    min(case when payment_status = "completePaymentRequested" then day end) as complete_payment_requested,
    min(case when payment_status = "completePaymentInProgress" then day end) as complete_payment_in_progress,
    min(case when payment_status = "completePaymentAcquired" then day end) as complete_payment_acquired,
    min(case when payment_status = "merchantAcquiredPayment" then day end) as merchant_acquired_payment
    from added_data a
    left join 
    (
        select *, first_value(payment_status) over (partition by _id, orderId, merchantId order by day desc) as last_payment_status
        from
    (select distinct _id, friendlyId, orderId, merchantId, manDays, daysAfterQC, day, s.status as payment_status
    from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }} o
    left join statuses s on o._id = s.id
    )
        )m on m.friendlyId = a.friendly_id
group by friendly_id, orderId, merchantId
),

merchant_orders as 
(select merchant_order_id, order_id, merchant_id, man_days,
    row_number() over (partition by order_id order by man_days desc) = 1 as longest_order,
    count(merchant_order_id) over (partition by order_id) = 1 as one_merchant_order,
    last_payment_status,
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
    max(last_payment_status) as last_payment_status,
    max(manDays) as man_days,
    min(date(case when payment_status = "noOperationsStarted" then day end)) as no_operations_started,
    min(date(case when payment_status = "advancePaymentRequested" then day end)) as advance_payment_requested,
    min(date(case when payment_status = "advancePaymentInProgress" then day end)) as advance_payment_in_progress,
    min(date(case when payment_status = "advancePaymentAcquired" then day end)) as advance_payment_acquired,
    min(date(case when payment_status = "manufacturingAndQcInProgress" then day end)) as manufacturing_and_qc_in_progress,
    min(date(case when payment_status = "remainingPaymentRequested" then day end)) as remaining_payment_requested,
    min(date(case when payment_status = "remainingPaymentInProgress" then day end)) as remaining_payment_in_progress,
    min(date(case when payment_status = "remainingPaymentAcquired" then day end)) as remaining_payment_acquired,
    min(date(case when payment_status = "completePaymentRequested" then day end)) as complete_payment_requested,
    min(date(case when payment_status = "completePaymentInProgress" then day end)) as complete_payment_in_progress,
    min(date(case when payment_status = "completePaymentAcquired" then day end)) as complete_payment_acquired,
    min(date(case when payment_status = "merchantAcquiredPayment" then day end)) as merchant_acquired_payment
from 
(
        select *, first_value(payment_status) over (partition by _id, orderId, merchantId order by day desc) as last_payment_status
        from
    (
    select distinct _id, orderId, merchantId, manDays, daysAfterQC, day, s.status as payment_status
from {{ source('mongo', 'b2b_core_merchant_orders_v2_daily_snapshot') }} o
left join statuses s on o._id = s.id
)
    )
group by _id, orderId, merchantId
    union all 
    select * from added_orders
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
        MIN(case when sub_status = "delivered" then o.event_ts_msk end) as delivered,
        MIN(case when status = "claim" then o.event_ts_msk end) as claimed,
        MIN(case when status = "closed" then o.event_ts_msk end) as closed
    FROM {{ ref('fact_order_change') }} o
    group by order_id
),

orders as 
    (
        select distinct
        order_id, last_order_status as status, last_order_sub_status as sub_status
    FROM {{ ref('fact_order') }} o
        where next_effective_ts_msk is null
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
    orders.status,
    orders.sub_status,
    last_payment_status,
    
    coalesce(shipping, closed) as manufacturing_ended,
    man_days,
    delivered,
        
    case when claimed is not null then 'claimed'
        when cancelled is not null and cancelled > manufacturing then 'cancelled' 
        when closed is not null then 'closed'
        when delivered is not null then 'delivered'
        when shipping is not null then 'shipping'
        when manufacturing is not null then 'manufacturing'
        end as claim
from merchant_orders 
left join order_statuses on merchant_orders.order_id = order_statuses.order_id
left join orders on order_statuses.order_id = orders.order_id
