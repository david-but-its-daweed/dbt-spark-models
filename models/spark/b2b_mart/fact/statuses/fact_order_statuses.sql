{{ config(
    schema='b2b_mart',
    materialized='table',
    file_format='parquet',
    meta = {
      'model_owner' : '@amitiushkina',
      'bigquery_load': 'true'
    }
) }}


WITH orders AS (
    select distinct
        deal_id,
        user_id,
        o.order_id,
        order_friendly_id,
        owner_email,
        owner_role,
        created_ts_msk
    FROM {{ ref('dim_deal_products') }} AS d
    LEFT JOIN (
        SELECT order_id, created_ts_msk
        FROM {{ ref('fact_order') }}) AS o ON d.order_id = o.order_id
),


orders_hist AS (
    SELECT
        order_id,
        
        MAX(case when current_status then status end) as current_status,
        MAX(case when current_status then sub_status end) as current_sub_status,
        
        MIN(IF(status = 'selling', event_ts_msk, NULL)) AS selling,
        
        MIN(IF(status = 'selling' AND sub_status = 'new', event_ts_msk, NULL)) AS new,
        MIN(IF(status = 'selling' AND sub_status = 'priceEstimation', event_ts_msk, NULL)) AS price_estimation,
        MIN(IF(status = 'selling' AND sub_status = 'negotiation', event_ts_msk, NULL)) AS negotiation,
        MIN(IF(status = 'selling' AND sub_status = 'finalPricing', event_ts_msk, NULL)) AS final_pricing,
        MIN(IF(status = 'selling' AND sub_status = 'signingAndPayment', event_ts_msk, NULL)) AS signing_and_payment,
        
        MIN(IF(status = 'manufacturing', event_ts_msk, NULL)) AS manufacturing,
        
        MIN(IF(status = 'manufacturing' AND sub_status = 'client2BrokerPaymentSent', event_ts_msk, NULL)) AS client_to_broker_payment_sent,
        MIN(IF(status = 'manufacturing' AND sub_status = 'brokerPaymentReceived', event_ts_msk, NULL)) AS broker_payment_received,
        MIN(IF(status = 'manufacturing' AND sub_status = 'broker2JoomSIAPaymentSent', event_ts_msk, NULL)) AS broker_to_joom_sia__payment_sent,
        MIN(IF(status = 'manufacturing' AND sub_status = 'joomSIAPaymentReceived', event_ts_msk, NULL)) AS joom_sia_payment_received,
        
        MIN(IF(status = 'shipping', event_ts_msk, NULL)) AS shipping,
        
        MIN(IF(status = 'shipping' AND sub_status = 'pickupRequestSentToLogisticians', event_ts_msk, NULL)) AS pickup_request_sent_to_logisticians,
        MIN(IF(status = 'shipping' AND sub_status = 'pickedUpByLogisticians', event_ts_msk, NULL)) AS picked_up_by_logisticians,
        MIN(IF(status = 'shipping' AND sub_status = 'arrivedAtLogisticsWarehouse', event_ts_msk, NULL)) AS arrived_at_logistics_warehouse,
        MIN(IF(status = 'shipping' AND sub_status = 'departedFromLogisticsWarehouse', event_ts_msk, NULL)) AS departed_from_logistics_warehouse,
        MIN(IF(status = 'shipping' AND sub_status = 'arrivedAtStateBorder', event_ts_msk, NULL)) AS arrived_at_state_border,
        MIN(IF(status = 'shipping' AND sub_status = 'departedFromStateBorder', event_ts_msk, NULL)) AS departed_from_state_border,
        MIN(IF(status = 'shipping' AND sub_status = 'arrivedAtDestinations', event_ts_msk, NULL)) AS arrived_at_destinations,
        MIN(IF(status = 'shipping' AND sub_status = 'documentReceived', event_ts_msk, NULL)) AS document_received,
        MIN(IF(status = 'shipping' AND sub_status = 'customsDeclarationFiled', event_ts_msk, NULL)) AS customs_declaration_filed,
        MIN(IF(status = 'shipping' AND sub_status = 'customsDeclarationReleased', event_ts_msk, NULL)) AS customs_declaration_released,
        MIN(IF(status = 'shipping' AND sub_status = 'uploadToTemporaryWarehouse', event_ts_msk, NULL)) AS upload_to_temporary_warehouse,
        MIN(IF(status = 'shipping' AND sub_status = 'delivering', event_ts_msk, NULL)) AS delivering,
        MIN(IF(status = 'shipping' AND sub_status = 'delivered', event_ts_msk, NULL)) AS delivered,
        MIN(IF(status = 'shipping' AND sub_status = 'requestedClosingDocuments', event_ts_msk, NULL)) AS requested_closing_documents,
        MIN(IF(status = 'shipping' AND sub_status = 'receivedClosingDocuments', event_ts_msk, NULL)) AS received_closing_documents,
        
        MIN(IF(status = 'cancelled', event_ts_msk, NULL)) AS cancelled,

        MIN(IF(status = 'claim', event_ts_msk, NULL)) AS claim,

        MIN(IF(status = 'done', event_ts_msk, NULL)) AS done
    
    FROM {{ ref('fact_order_statuses_change') }}
    GROUP BY 1
)

SELECT DISTINCT
    oh.*,
    o.deal_id,
    o.user_id,
    o.order_friendly_id,
    o.owner_email,
    o.owner_role,
    o.created_ts_msk
FROM orders AS o
LEFT JOIN orders_hist AS oh ON o.order_id = oh.order_id
